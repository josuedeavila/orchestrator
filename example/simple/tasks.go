package simple

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/josuedeavila/taskflow"
)

type Service struct {
	client *http.Client
	logger taskflow.Logger
}

func NewService(client *http.Client, logger taskflow.Logger) *Service {
	return &Service{
		client: client,
		logger: logger,
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

func (s *Service) GetEvents() *taskflow.Task[any, []*Event] {
	return taskflow.NewTask("get_events", func(ctx context.Context, _ any) ([]*Event, error) {
		inner := func(ctx context.Context) error {
			s.logger.Log("🔍 Fetching events")
			time.Sleep(2 * time.Second) // Simulate delay in fetching events

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/events", nil)
			if err != nil {
				s.logger.Log("❌ Error creating request: ", err)
				return err
			}
			res, err := s.client.Do(req)
			if err != nil {
				s.logger.Log("❌ Error fetching events: ", err)
				return err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d when fetching events", res.StatusCode)
				s.logger.Log("❌ Error fetching events: ", err)
				return err
			}
			s.logger.Log("✅ Events obtained successfully")
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
}

func (s *Service) GetCredentials() *taskflow.Task[[]*Event, *EventsAndCredentials] {
	return taskflow.NewTask("get_credentials", func(ctx context.Context, events []*Event) (*EventsAndCredentials, error) {
		inner := func(ctx context.Context) error {
			s.logger.Log("🔍 Fetching credentials")
			time.Sleep(2 * time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/credentials", nil)
			if err != nil {
				s.logger.Log("❌ Error creating request: ", err)
				return err
			}

			res, err := s.client.Do(req)
			if err != nil {
				s.logger.Log("❌ Error fetching credentials: ", err)
				return err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d when fetching credentials", res.StatusCode)
				s.logger.Log("❌ Error fetching credentials: ", err)
				return err
			}
			s.logger.Log("✅ Credentials obtained successfully")
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
	})
}

func (s *Service) SendToChannel() *taskflow.Task[*EventsAndCredentials, []*Event] {
	return taskflow.NewTask("send_to_channel", func(ctx context.Context, input *EventsAndCredentials) ([]*Event, error) {
		inner := func(ctx context.Context) error {
			s.logger.Log("📤 Sending events to channel")
			time.Sleep(2 * time.Second)
			b, err := json.Marshal(input.Events)
			if err != nil {
				s.logger.Log("❌ Error serializing events: ", err)
				return err
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8080/send", bytes.NewBuffer(b))
			req.Header.Set("Authorization", "Bearer "+input.Credentials.Token)
			res, err := s.client.Do(req)
			if err != nil {
				s.logger.Log("❌ Error sending events: ", err)
				return err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d when sending events", res.StatusCode)
				s.logger.Log("❌ Error sending events: ", err)
				return err
			}
			s.logger.Log("✅ Events sent successfully")
			// Simulate successful sending
			return nil
		}

		err := taskflow.Retry(ctx, inner, 3, 2*time.Second)
		if err != nil {
			return nil, err
		}

		return input.Events, nil
	})
}

func (s *Service) UpdateOffer() *taskflow.FanOutTask[*Event, string] {
	return &taskflow.FanOutTask[*Event, string]{
		Name: "update_offer_fanout",
		Generate: func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			s.logger.Log("FanOutTask: Generating fan-out functions for offer updates...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					s.logger.Log("FanOutTask: Processing offer update ", event.OfferID)

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/offers/%s", event.OfferID), nil)
					resp, err := s.client.Do(req)
					if err != nil {
						s.logger.Log("❌ Error updating offer ", event.OfferID, ": ", err)
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d when updating offer %s", resp.StatusCode, event.ID)
						s.logger.Log("❌ Error updating offer ", event.OfferID, ": ", err)
						return "", err
					}
					s.logger.Log("✔️ Offer %s updated successfully", event.OfferID)
					return event.OfferID, nil
				})
			}

			return fns, nil
		},
		FanIn: func(ctx context.Context, input []string) (string, error) {
			for _, result := range input {
				s.logger.Log("FanOutTask: Result received: ", result)
			}
			s.logger.Log("FanOutTask: Consolidating results...")
			return "success", nil
		},
	}
}

func (s *Service) UpdateEvent() *taskflow.FanOutTask[*Event, string] {
	return &taskflow.FanOutTask[*Event, string]{
		Name: "update_event_fanout",
		Generate: func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			s.logger.Log("FanOutTask: Generating fan-out functions for event updates...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					s.logger.Log("FanOutTask: Processing event update ", event.ID)

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/events/%s", event.ID), nil)
					resp, err := s.client.Do(req)
					if err != nil {
						s.logger.Log("❌ Error updating event ", event.ID, ": ", err)
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d when updating event %s", resp.StatusCode, event.ID)
						s.logger.Log("❌ Error updating event ", event.ID, ": ", err)
						return "", err
					}
					s.logger.Log("✔️ Event %s updated successfully", event.ID)
					return event.ID, nil
				})
			}

			return fns, nil
		},
		FanIn: func(ctx context.Context, input []string) (string, error) {
			for _, result := range input {
				s.logger.Log("FanOutTask: Result received: ", result)
			}
			s.logger.Log("FanOutTask: Consolidating results...")
			return "success", nil
		},
	}
}
