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
		MaxRetries: 0,
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

	time.Sleep(5 * time.Millisecond) // wait to task to be executed

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
		RetryDelay:      10 * time.Millisecond,
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

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected exacly 3 retries, got %d", count)
	}
	if !errorCallbackCalled.Load() {
		t.Errorf("expected OnError callback to be called")
	}
}

// Task que demora para testar timeout
type slowTask struct {
	taskflow.Task[any, any]
	duration time.Duration
}

func (s *slowTask) Run(ctx context.Context, _ any) (any, error) {
	select {
	case <-time.After(s.duration):
		return "completed", nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newSlowTask(duration time.Duration) *slowTask {
	return &slowTask{duration: duration}
}

// Logger para capturar mensagens nos testes
type testLogger struct {
	messages []string
}

func (l *testLogger) Log(v ...any) {
	l.messages = append(l.messages, fmt.Sprint(v...))
}

func TestPipelineConfigDefaults(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		return []taskflow.Executable{newDummyTask("result")}, nil
	}

	config := &orchestrator.PipelineConfig{
		Name:            "test-defaults",
		Interval:        time.Second,
		PipelineBuilder: builder,
	}

	err := o.AddPipeline(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error on start: %v", err)
	}

	err = o.Shutdown(1 * time.Second)
	if err != nil {
		t.Errorf("unexpected error on shutdown: %v", err)
	}
}

func TestConcurrencyLimits(t *testing.T) {
	logger := &testLogger{}
	o := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	var runningCount int32
	var maxConcurrent int32

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		current := atomic.AddInt32(&runningCount, 1)
		defer atomic.AddInt32(&runningCount, -1)

		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
		return []taskflow.Executable{newDummyTask("result")}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "concurrency-test",
		Interval:        10 * time.Millisecond,
		PipelineBuilder: builder,
		MaxConcurrency:  2,
		MaxRetries:      1,
		Enabled:         true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(250 * time.Millisecond)
	_ = o.Shutdown(1 * time.Second)

	if atomic.LoadInt32(&maxConcurrent) > 2 {
		t.Errorf("expected max concurrency of 2, got %d", maxConcurrent)
	}

	// Verificar se houve logs sobre limite de concorrência
	found := false
	for _, msg := range logger.messages {
		if fmt.Sprintf("Pipeline '%s' skipped - concurrency limit reached", "concurrency-test") == msg {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected concurrency limit message in logs")
	}
}

func TestEmptyPipelineTasks(t *testing.T) {
	logger := &testLogger{}
	o := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	var errorCallbackCalled atomic.Bool

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		return []taskflow.Executable{}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "empty-pipeline",
		Interval:        100 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      1,
		Enabled:         true,
		OnError: func(ctx context.Context, err error) {
			errorCallbackCalled.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	_ = o.Shutdown(1 * time.Second)

	found := false
	for _, msg := range logger.messages {
		if fmt.Sprintf("Pipeline '%s' failed on attempt 1/2: pipeline '%s' has no tasks", "empty-pipeline", "empty-pipeline") == msg {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected has no tasks message in logs")
	}

}

func TestDisabledPipelineSkipsExecution(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	var executionCount int32

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		atomic.AddInt32(&executionCount, 1)
		return []taskflow.Executable{newDummyTask("result")}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "disabled-pipeline",
		Interval:        50 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      1,
		Enabled:         false, // Pipeline desabilitado
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	_ = o.Shutdown(1 * time.Second)

	if atomic.LoadInt32(&executionCount) != 0 {
		t.Errorf("expected 0 executions for disabled pipeline, got %d", executionCount)
	}
}

func TestContextCancellation(t *testing.T) {
	logger := &testLogger{}
	o := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		return []taskflow.Executable{newSlowTask(1 * time.Second)}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "context-cancel-test",
		Interval:        100 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      1,
		Enabled:         true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(100 * time.Millisecond) // Esperar o contexto ser cancelado
	_ = o.Shutdown(1 * time.Second)

	// Verificar se houve log de cancelamento por contexto
	found := false
	for _, msg := range logger.messages {
		if fmt.Sprintf("Pipeline '%s' terminated by context", "context-cancel-test") == msg {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected context cancellation message in logs")
	}
}

func TestShutdownTimeout(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		return []taskflow.Executable{newSlowTask(1 * time.Second)}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "long-running",
		Interval:        10 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      1,
		Enabled:         true,
	})

	ctx := context.Background()
	_ = o.Start(ctx)

	// Shutdown com timeout muito pequeno
	err := o.Shutdown(1 * time.Nanosecond)
	if err == nil {
		t.Error("expected shutdown timeout error")
	}
}

func TestMultiplePipelinesExecution(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	var count1, count2 int32

	builder1 := func(ctx context.Context) ([]taskflow.Executable, error) {
		atomic.AddInt32(&count1, 1)
		return []taskflow.Executable{newDummyTask("result1")}, nil
	}

	builder2 := func(ctx context.Context) ([]taskflow.Executable, error) {
		atomic.AddInt32(&count2, 1)
		return []taskflow.Executable{newDummyTask("result2")}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "pipeline1",
		Interval:        50 * time.Millisecond,
		PipelineBuilder: builder1,
		MaxRetries:      1,
		Enabled:         true,
	})

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "pipeline2",
		Interval:        50 * time.Millisecond,
		PipelineBuilder: builder2,
		MaxRetries:      1,
		Enabled:         true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	_ = o.Shutdown(1 * time.Second)

	if atomic.LoadInt32(&count1) == 0 {
		t.Error("expected pipeline1 to execute at least once")
	}

	if atomic.LoadInt32(&count2) == 0 {
		t.Error("expected pipeline2 to execute at least once")
	}

	pipelines := o.ListPipelines()
	if len(pipelines) != 2 {
		t.Errorf("expected 2 pipelines, got %d", len(pipelines))
	}
}

func TestRemovePipelineWhileRunning(t *testing.T) {
	logger := &testLogger{}
	o := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	builder := func(ctx context.Context) ([]taskflow.Executable, error) {
		return []taskflow.Executable{newDummyTask("result")}, nil
	}

	_ = o.AddPipeline(&orchestrator.PipelineConfig{
		Name:            "to-be-removed",
		Interval:        50 * time.Millisecond,
		PipelineBuilder: builder,
		MaxRetries:      1,
		Enabled:         true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = o.Start(ctx)
	time.Sleep(25 * time.Millisecond) // Deixar pipeline começar a executar

	// Remover pipeline durante execução
	o.RemovePipeline("to-be-removed")

	pipelines := o.ListPipelines()
	if len(pipelines) != 0 {
		t.Errorf("expected 0 pipelines after removal, got %d", len(pipelines))
	}

	_ = o.Shutdown(1 * time.Second)

	// Verificar se houve log de remoção
	found := false
	for _, msg := range logger.messages {
		if fmt.Sprintf("Pipeline '%s' removed", "to-be-removed") == msg {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected removal message in logs")
	}
}

func TestEnableDisableNonexistentPipeline(t *testing.T) {
	o := orchestrator.NewPipelineOrchestrator()

	err := o.EnablePipeline("nonexistent")
	if err == nil {
		t.Error("expected error when enabling nonexistent pipeline")
	}

	err = o.DisablePipeline("nonexistent")
	if err == nil {
		t.Error("expected error when disabling nonexistent pipeline")
	}
}
