package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/josuedeavila/orchestrator"
	"github.com/josuedeavila/orchestrator/example/simple"
	"github.com/josuedeavila/taskflow"
)

type logger struct {
	*slog.Logger
}

func (l *logger) Log(args ...any) {
	l.Logger.Info(fmt.Sprint(args...))
}

func main() {
	logger := &logger{
		Logger: slog.Default(),
	}
	// Create the orchestrator
	orc := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	// Monitoring pipeline (initially disabled)
	updateOfferConfig := &orchestrator.PipelineConfig{
		Name:            "update_offer",
		Interval:        20 * time.Second,
		MaxConcurrency:  1,
		MaxRetries:      0,
		Timeout:         100 * time.Second,
		Enabled:         false,
		PipelineBuilder: buildPipeline(logger),
		OnSuccess: func(ctx context.Context, result any) {
			if status, ok := result.(string); ok {
				logger.Log("💚 Pipeline executed with", status)
			}
		},
		OnError: func(ctx context.Context, err error) {
			logger.Log("❌ Error in pipeline execution:", err)
		},
	}

	// Add the pipelines
	configs := []*orchestrator.PipelineConfig{
		updateOfferConfig,
	}
	for _, config := range configs {
		if err := orc.AddPipeline(config); err != nil {
			logger.Log("Error adding pipeline", config.Name, ": ", err)
		}
	}

	// Start the orchestrator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orc.Start(ctx); err != nil {
		logger.Log("Error starting orchestrator: ", err)
	}

	logger.Log("🚀 Orchestrator started!")
	logger.Log("📋 Pipelines: ", orc.ListPipelines())

	// Demonstrate dynamic pipeline control
	go func() {
		time.Sleep(3 * time.Second)
		orc.EnablePipeline("update_offer")
	}()

	// Run for 2 minutes
	time.Sleep(2 * time.Minute)

	// Graceful shutdown
	logger.Log("🛑 Shutting down orchestrator...")
	cancel()

	if err := orc.Shutdown(5 * time.Second); err != nil {
		logger.Log("Error during shutdown: ", err)
	} else {
		logger.Log("✅ Orchestrator shutdown successfully!")
	}
}

func buildPipeline(logger taskflow.Logger) func(ctx context.Context) ([]taskflow.Executable, error) {
	svc := simple.NewService(
		http.DefaultClient,
		logger,
	)
	return func(ctx context.Context) ([]taskflow.Executable, error) {
		getEventsTask := svc.GetEvents()
		getCredentialsTask := svc.GetCredentials()
		sendToChannelTask := svc.SendToChannel()
		updateOfferfanOutTask := svc.UpdateOffer().ToTask()
		updateEventfanOutTask := svc.UpdateEvent().ToTask()

		return []taskflow.Executable{
			getEventsTask,
			getCredentialsTask.After(getEventsTask),
			sendToChannelTask.After(getCredentialsTask),
			updateOfferfanOutTask.After(sendToChannelTask),
			updateEventfanOutTask.After(sendToChannelTask),
		}, nil
	}
}
