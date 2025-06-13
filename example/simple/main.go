package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/josuedeavila/orchestrator"
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
		MaxRetries:      1,
		Timeout:         100 * time.Second,
		Enabled:         false,
		PipelineBuilder: buildPipeline(logger),
		OnSuccess: func(ctx context.Context, result any) {
			if status, ok := result.(string); ok {
				logger.Log(fmt.Sprintf("💚 Pipeline executed with %s", status))
			}
		},
		OnError: func(ctx context.Context, err error) {
			logger.Log(fmt.Sprintf("❌ Error in pipeline execution: %v", err))
		},
	}

	// Add the pipelines
	configs := []*orchestrator.PipelineConfig{
		updateOfferConfig,
	}
	for _, config := range configs {
		if err := orc.AddPipeline(config); err != nil {
			log.Fatalf("Error adding pipeline %s: %v", config.Name, err)
		}
	}

	// Start the orchestrator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orc.Start(ctx); err != nil {
		log.Fatalf("Error starting orchestrator: %v", err)
	}

	logger.Log("🚀 Orchestrator started!")
	logger.Log(fmt.Sprintf("📋 Pipelines: %v", orc.ListPipelines()))

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
		logger.Log(fmt.Sprintf("Error during shutdown: %v", err))
	} else {
		logger.Log("✅ Orchestrator shutdown successfully!")
	}
}

func buildPipeline(logger taskflow.Logger) func(ctx context.Context) ([]taskflow.Executable, error) {
	client := http.DefaultClient
	return func(ctx context.Context) ([]taskflow.Executable, error) {
		getEvents := taskflow.NewTask("get_events", func(ctx context.Context, _ any) ([]*Event, error) {

			inner := func(ctx context.Context) error {
				logger.Log("🔍 Fetching events")
				time.Sleep(2 * time.Second) // Simulate delay in fetching events

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/events", nil)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error creating request: %v", err))
					return err
				}
				res, err := client.Do(req)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error fetching events: %v", err))
					return err
				}
				defer res.Body.Close()
				if res.StatusCode != http.StatusOK {
					err := fmt.Errorf("status %d when fetching events", res.StatusCode)
					logger.Log(fmt.Sprintf("❌ Error fetching events: %v", err))
					return err
				}
				logger.Log("✅ Events obtained successfully")
				return nil
			}

			err := taskflow.Retry(ctx, inner, 3, 2*time.Second)
			if err != nil {
				return nil, err
			}

			return []*Event{
				{ID: "1", Name: "Event 1", OfferID: "offer-123"},
				{ID: "2", Name: "Event 2", OfferID: "offer-456"},
				{ID: "3", Name: "Event 3", OfferID: "offer-789"},
			}, nil
		})

		getCredentials := taskflow.NewTask("get_credentials", func(ctx context.Context, events []*Event) (*EventsAndCredentials, error) {
			inner := func(ctx context.Context) error {
				logger.Log("🔍 Fetching credentials")
				time.Sleep(2 * time.Second)
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/credentials", nil)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error creating request: %v", err))
					return err
				}

				res, err := client.Do(req)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error fetching credentials: %v", err))
					return err
				}
				defer res.Body.Close()
				if res.StatusCode != http.StatusOK {
					err := fmt.Errorf("status %d when fetching credentials", res.StatusCode)
					logger.Log(fmt.Sprintf("❌ Error fetching credentials: %v", err))
					return err
				}
				logger.Log("✅ Credentials obtained successfully")
				return nil
			}

			err := taskflow.Retry(ctx, inner, 3, 2*time.Second)
			if err != nil {
				return nil, err
			}

			var creds Credentials
			// Simulate credential filling
			creds.Token = "example-token"
			return &EventsAndCredentials{
				Events:      events,
				Credentials: &creds,
			}, nil
		}).After(getEvents)

		sendToChannel := taskflow.NewTask("send_to_channel", func(ctx context.Context, input *EventsAndCredentials) ([]*Event, error) {
			inner := func(ctx context.Context) error {
				logger.Log("📤 Sending events to channel")
				time.Sleep(2 * time.Second)
				b, err := json.Marshal(input.Events)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error serializing events: %v", err))
					return err
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8080/send", bytes.NewBuffer(b))
				req.Header.Set("Authorization", "Bearer "+input.Credentials.Token)
				res, err := client.Do(req)
				if err != nil {
					logger.Log(fmt.Sprintf("❌ Error sending events: %v", err))
					return err
				}
				defer res.Body.Close()
				if res.StatusCode != http.StatusOK {
					err := fmt.Errorf("status %d when sending events", res.StatusCode)
					logger.Log(fmt.Sprintf("❌ Error sending events: %v", err))
					return err
				}
				logger.Log("✅ Events sent successfully")
				// Simulate successful sending
				return nil
			}

			err := taskflow.Retry(ctx, inner, 3, 2*time.Second)
			if err != nil {
				return nil, err
			}

			return input.Events, nil
		}).After(getCredentials)

		updateOfferGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			logger.Log("FanOutTask: Generating fan-out functions for offer updates...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					logger.Log(fmt.Sprintf("FanOutTask: Processing offer update %s...", event.OfferID))

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/offers/%s", event.OfferID), nil)
					resp, err := client.Do(req)
					if err != nil {
						logger.Log(fmt.Sprintf("❌ Error updating offer %s: %v", event.OfferID, err))
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d when updating offer %s", resp.StatusCode, event.ID)
						logger.Log(fmt.Sprintf("❌ Error updating offer %s: %v", event.OfferID, err))
						return "", err
					}
					logger.Log(fmt.Sprintf("✔️ Offer %s updated successfully", event.OfferID))
					return event.OfferID, nil
				})
			}

			return fns, nil
		}

		updateOfferfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				logger.Log(fmt.Sprintf("FanOutTask: Result received: %s", result))
			}
			logger.Log("FanOutTask: Consolidating results...")
			return "success", nil
		}

		updateOfferfanOutTask := &taskflow.FanOutTask[*Event, string]{
			Name:     "update_offer_fanout",
			Generate: updateOfferGenerateFunc,
			FanIn:    updateOfferfanInFunc,
		}

		updateEventGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			logger.Log("FanOutTask: Generating fan-out functions for event updates...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					logger.Log(fmt.Sprintf("FanOutTask: Processing event update %s...", event.ID))

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/events/%s", event.ID), nil)
					resp, err := client.Do(req)
					if err != nil {
						logger.Log(fmt.Sprintf("❌ Error updating event %s: %v", event.ID, err))
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d when updating event %s", resp.StatusCode, event.ID)
						logger.Log(fmt.Sprintf("❌ Error updating event %s: %v", event.ID, err))
						return "", err
					}
					logger.Log(fmt.Sprintf("✔️ Event %s updated successfully", event.ID))
					return event.ID, nil
				})
			}

			return fns, nil
		}

		updateEventfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				logger.Log(fmt.Sprintf("FanOutTask: Result received: %s", result))
			}
			logger.Log("FanOutTask: Consolidating results...")
			return "success", nil
		}

		updateEventfanOutTask := &taskflow.FanOutTask[*Event, string]{
			Name:     "update_event_fanout",
			Generate: updateEventGenerateFunc,
			FanIn:    updateEventfanInFunc,
		}

		// Convert FanOutTask to a normal Task
		fanOutUpdateOfferTask := updateOfferfanOutTask.ToTask().After(sendToChannel)
		fanOutUpdateEventTask := updateEventfanOutTask.ToTask().After(sendToChannel)

		return []taskflow.Executable{
			getEvents,
			getCredentials,
			sendToChannel,
			fanOutUpdateOfferTask,
			fanOutUpdateEventTask,
		}, nil
	}

}

type Credentials struct {
	Token string
}

type Event struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	OfferID string `json:"offer_id,omitempty"`
}

type EventsAndCredentials struct {
	Events      []*Event
	Credentials *Credentials
}
