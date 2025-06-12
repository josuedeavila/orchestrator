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
	// Cria o orquestrador
	orc := orchestrator.NewPipelineOrchestrator().WithLogger(logger)

	// Pipeline de monitoramento (inicialmente desabilitada)
	updateOfferConfig := &orchestrator.PipelineConfig{
		Name:            "update_offer",
		Interval:        10 * time.Second,
		MaxConcurrency:  1,
		MaxRetries:      2,
		Timeout:         10 * time.Second,
		Enabled:         false,
		PipelineBuilder: buildPipeline(logger),
		OnSuccess: func(ctx context.Context, result any) {
			if status, ok := result.(string); ok {
				logger.Log(fmt.Sprintf("💚 Pipeline executada com %s", status))
			}
		},
		OnError: func(ctx context.Context, err error) {
			logger.Log(fmt.Sprintf("❌ Erro na execução da pipeline: %v", err))
		},
	}

	// Adiciona as pipelines
	configs := []*orchestrator.PipelineConfig{
		updateOfferConfig,
	}
	for _, config := range configs {
		if err := orc.AddPipeline(config); err != nil {
			log.Fatalf("Erro ao adicionar pipeline %s: %v", config.Name, err)
		}
	}

	// Inicia o orquestrador
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orc.Start(ctx); err != nil {
		log.Fatalf("Erro ao iniciar orquestrador: %v", err)
	}

	logger.Log("🚀 Orquestrador iniciado!")
	logger.Log(fmt.Sprintf("📋 Pipelines: %v", orc.ListPipelines()))

	// Demonstra controle dinâmico das pipelines
	go func() {
		time.Sleep(3 * time.Second)
		orc.EnablePipeline("update_offer")
	}()

	// Executa por 2 minutos
	time.Sleep(2 * time.Minute)

	// Encerra graciosamente
	logger.Log("🛑 Encerrando orquestrador...")
	cancel()

	if err := orc.Shutdown(5 * time.Second); err != nil {
		logger.Log(fmt.Sprintf("Erro no shutdown: %v", err))
	} else {
		logger.Log("✅ Orquestrador encerrado com sucesso!")
	}
}

func buildPipeline(logger taskflow.Logger) func(ctx context.Context) ([]taskflow.Executable, error) {
	client := http.DefaultClient
	return func(ctx context.Context) ([]taskflow.Executable, error) {
		getEvents := taskflow.NewTask("get_events", func(ctx context.Context, _ any) ([]*Event, error) {
			logger.Log("🔍 Buscando eventos")
			time.Sleep(2 * time.Second) // Simula um atraso na busca de eventos
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/events", nil)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao criar requisição: %v", err))
				return nil, err
			}
			res, err := client.Do(req)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao buscar eventos: %v", err))
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao buscar eventos", res.StatusCode)
				logger.Log(fmt.Sprintf("❌ Erro ao buscar eventos: %v", err))
				return nil, err
			}
			logger.Log("✅ Eventos obtidos com sucesso")
			// Simula o processamento dos eventos
			return []*Event{
				{ID: "1", Name: "Evento 1", OfferID: "offer-123"},
				{ID: "2", Name: "Evento 2", OfferID: "offer-456"},
				{ID: "3", Name: "Evento 3", OfferID: "offer-789"},
			}, nil
		})

		getCredentials := taskflow.NewTask("get_credentials", func(ctx context.Context, events []*Event) (*EventsAndCredentials, error) {
			logger.Log("🔍 Buscando credenciais")
			time.Sleep(2 * time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/credentials", nil)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao criar requisição: %v", err))
				return nil, err
			}

			res, err := client.Do(req)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao buscar credenciais: %v", err))
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao buscar credenciais", res.StatusCode)
				logger.Log(fmt.Sprintf("❌ Erro ao buscar credenciais: %v", err))
				return nil, err
			}
			logger.Log("✅ Credenciais obtidas com sucesso")

			var creds Credentials
			// Simula o preenchimento das credenciais
			creds.Token = "example-token"
			return &EventsAndCredentials{
				Events:      events,
				Credentials: &creds,
			}, nil
		}).After(getEvents)

		sendToChannel := taskflow.NewTask("send_to_channel", func(ctx context.Context, input *EventsAndCredentials) ([]*Event, error) {
			logger.Log("📤 Enviando eventos para o canal")
			time.Sleep(2 * time.Second)
			b, err := json.Marshal(input.Events)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao serializar eventos: %v", err))
				return nil, err
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8080/send", bytes.NewBuffer(b))
			req.Header.Set("Authorization", "Bearer "+input.Credentials.Token)
			res, err := client.Do(req)
			if err != nil {
				logger.Log(fmt.Sprintf("❌ Erro ao enviar eventos: %v", err))
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao enviar eventos", res.StatusCode)
				logger.Log(fmt.Sprintf("❌ Erro ao enviar eventos: %v", err))
				return nil, err
			}
			logger.Log("✅ Eventos enviados com sucesso")
			// Simula o envio bem-sucedido

			return input.Events, nil
		}).After(getCredentials)

		updateOfferGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			logger.Log("FanOutTask: Gerando funções de fan-out para atualização de ofertas...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					logger.Log(fmt.Sprintf("FanOutTask: Processando atualização de oferta %s...", event.OfferID))

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/offers/%s", event.OfferID), nil)
					resp, err := client.Do(req)
					if err != nil {
						logger.Log(fmt.Sprintf("❌ Erro ao atualizar offerta %s: %v", event.OfferID, err))
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d ao atualizar oferta %s", resp.StatusCode, event.ID)
						logger.Log(fmt.Sprintf("❌ Erro ao atualizar oferta %s: %v", event.OfferID, err))
						return "", err
					}
					logger.Log(fmt.Sprintf("✔️ Oferta %s atualizado com sucesso", event.OfferID))
					return event.OfferID, nil
				})
			}

			return fns, nil
		}

		updateOfferfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				logger.Log(fmt.Sprintf("FanOutTask: Resultado recebido: %s", result))
			}
			logger.Log("FanOutTask: Consolidando resultados...")
			return "success", nil
		}

		updateOfferfanOutTask := &taskflow.FanOutTask[*Event, string]{
			Name:     "update_offer_fanout",
			Generate: updateOfferGenerateFunc,
			FanIn:    updateOfferfanInFunc,
		}

		updateEventGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			logger.Log("FanOutTask: Gerando funções de fan-out para atualização de evenos...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					logger.Log(fmt.Sprintf("FanOutTask: Processando atualização de evento %s...", event.ID))

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/events/%s", event.ID), nil)
					resp, err := client.Do(req)
					if err != nil {
						logger.Log(fmt.Sprintf("❌ Erro ao atualizar evento %s: %v", event.ID, err))
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d ao atualizar evento %s", resp.StatusCode, event.ID)
						logger.Log(fmt.Sprintf("❌ Erro ao atualizar evento %s: %v", event.ID, err))
						return "", err
					}
					logger.Log(fmt.Sprintf("✔️ Evento %s atualizado com sucesso", event.ID))
					return event.ID, nil
				})
			}

			return fns, nil
		}

		updateEventfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				logger.Log(fmt.Sprintf("FanOutTask: Resultado recebido: %s", result))
			}
			logger.Log("FanOutTask: Consolidando resultados...")
			return "success", nil
		}

		updateEventfanOutTask := &taskflow.FanOutTask[*Event, string]{
			Name:     "update_event_fanout",
			Generate: updateEventGenerateFunc,
			FanIn:    updateEventfanInFunc,
		}

		// Converte o FanOutTask em um Task normal
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
