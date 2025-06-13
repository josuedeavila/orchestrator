package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/josuedeavila/orchestrator"
	"github.com/josuedeavila/taskflow"
)

type dummyTask struct {
	taskflow.Task[any, any]
	result any
}

func (d *dummyTask) Run(_ context.Context, _ any) (any, error) {
	d.result = any("result")
	return d.result, nil
}

func newDummyTask(result any) *dummyTask {
	return &dummyTask{result: result}
}

type dummyLogger struct{}

func (l *dummyLogger) Log(v ...any) {
	fmt.Println(v...)
}

func TestAddPipelineValidation(t *testing.T) {

	t.Run("Error", func(t *testing.T) {

		o := orchestrator.NewPipelineOrchestrator()

		err := o.AddPipeline(&orchestrator.PipelineConfig{
			Name: "",
		})
		if err == nil {
			t.Errorf("expected error for empty name, got nil")
		}

		err = o.AddPipeline(&orchestrator.PipelineConfig{
			Name:     "p1",
			Interval: 0,
		})
		if err == nil {
			t.Errorf("expected error for zero interval, got nil")
		}

		err = o.AddPipeline(&orchestrator.PipelineConfig{
			Name:            "p1",
			Interval:        time.Second,
			PipelineBuilder: nil,
		})
		if err == nil {
			t.Errorf("expected error for nil PipelineBuilder, got nil")
		}
	})

	t.Run("Success", func(t *testing.T) {
		o := orchestrator.NewPipelineOrchestrator()

		builder := func(ctx context.Context) ([]taskflow.Executable, error) {
			return []taskflow.Executable{newDummyTask("done")}, nil
		}

		err := o.AddPipeline(&orchestrator.PipelineConfig{
			Name:            "p1",
			Interval:        time.Millisecond * 100,
			MaxRetries:      1,
			PipelineBuilder: builder,
			Enabled:         true,
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Success with logger", func(t *testing.T) {
		o := orchestrator.NewPipelineOrchestrator().WithLogger(&dummyLogger{})

		builder := func(ctx context.Context) ([]taskflow.Executable, error) {
			return []taskflow.Executable{newDummyTask("done")}, nil
		}

		err := o.AddPipeline(&orchestrator.PipelineConfig{
			Name:            "p1",
			Interval:        time.Millisecond * 100,
			MaxRetries:      1,
			PipelineBuilder: builder,
			Enabled:         true,
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestEnableDisablePipeline(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	name := "pipeline1"
	err := o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            name,
		Interval:        time.Second,
		PipelineBuilder: func(ctx context.Context) ([]taskflow.Executable, error) { return nil, nil },
	})
	if err != nil {
		t.Fatalf("error adding pipeline: %v", err)
	}

	err = o.EnablePipeline(name)
	if err != nil {
		t.Errorf("expected success enabling pipeline, got: %v", err)
	}

	err = o.DisablePipeline(name)
	if err != nil {
		t.Errorf("expected success disabling pipeline, got: %v", err)
	}

	err = o.EnablePipeline("nonexistent")
	if err == nil {
		t.Errorf("expected error enabling nonexistent pipeline")
	}
}

func TestListPipelines(t *testing.T) {
	t.Run("With pilelines", func(t *testing.T) {
		o := orchestrator.NewPipelineOrchestrator()

		_ = o.AddPipeline(&orchestrator.PipelineConfig{
			Name:            "pipelineX",
			Interval:        time.Second,
			PipelineBuilder: func(ctx context.Context) ([]taskflow.Executable, error) { return nil, nil },
		})

		names := o.ListPipelines()
		if len(names) != 1 || names[0] != "pipelineX" {
			t.Errorf("unexpected pipeline list: %v", names)
		}
	})

	t.Run("After remove pipelines", func(t *testing.T) {
		o := orchestrator.NewPipelineOrchestrator()

		_ = o.AddPipeline(&orchestrator.PipelineConfig{
			Name:            "pipelineX",
			Interval:        time.Second,
			PipelineBuilder: func(ctx context.Context) ([]taskflow.Executable, error) { return nil, nil },
		})

		o.RemovePipeline("pipelineX")

		names := o.ListPipelines()
		if len(names) != 0 {
			t.Errorf("unexpected pipeline list: %v", names)
		}
	})
}

func TestStartAndShutdown(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	var count int32
	var successCallback atomic.Bool

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:     "p-runner",
		Interval: 10 * time.Millisecond,
		PipelineBuilder: func(ctx context.Context) ([]taskflow.Executable, error) {
			atomic.AddInt32(&count, 1)
			return []taskflow.Executable{newDummyTask("ok")}, nil
		},
		MaxRetries: 1,
		Enabled:    true,
		OnSuccess: func(ctx context.Context, result any) {
			successCallback.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := o.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error on start: %v", err)
	}

	time.Sleep(10 * time.Millisecond) // wait to task to be executed

	err = o.Shutdown(1 * time.Second)
	if err != nil {
		t.Errorf("unexpected error on shutdown: %v", err)
	}

	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("expected exacly 1 execution, got %d", count)
	}

	if !successCallback.Load() {
		t.Error("expected sucess callback to be called")
	}
}

func TestPipelineRetry(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	var count int32

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		atomic.AddInt32(&count, 1)
		return nil, errors.New("build fail")
	}

	var errorCallbackCalled atomic.Bool

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "retry-test",
		Interval:        100 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      2,
		RetryDelay:      40 * time.Millisecond,
		Timeout:         100 * time.Millisecond,
		Enabled:         true,
		OnError: func(ctx context.Context, err error) {
			errorCallbackCalled.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(80 * time.Millisecond)
	_ = o.Shutdown(1 * time.Second)

	if atomic.LoadInt32(&count) != 2 {
		t.Errorf("expected exacly 2 retries, got %d", count)
	}
	if !errorCallbackCalled.Load() {
		t.Errorf("expected OnError callback to be called")
	}
}
