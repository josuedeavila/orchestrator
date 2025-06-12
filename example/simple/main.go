package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/josuedeavila/orchestrator"
	"github.com/josuedeavila/taskflow"
)

func main() {
	// Cria o orquestrador
	orc := orchestrator.NewPipelineOrchestrator()

	// Pipeline de monitoramento (inicialmente desabilitada)
	updateOfferConfig := &orchestrator.PipelineConfig{
		Name:            "update_offer",
		Interval:        10 * time.Second,
		MaxConcurrency:  1,
		MaxRetries:      2,
		Timeout:         10 * time.Second,
		Enabled:         false,
		PipelineBuilder: buildPipeline(),
		OnSuccess: func(ctx context.Context, result any) {
			if status, ok := result.(string); ok {
				log.Printf("💚 Pipeline executada com %s", status)
			}
		},
		OnError: func(ctx context.Context, err error) {
			log.Printf("❌ Erro na execução da pipeline: %v", err)
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

	log.Println("🚀 Orquestrador iniciado!")
	log.Printf("📋 Pipelines: %v", orc.ListPipelines())

	// Demonstra controle dinâmico das pipelines
	go func() {
		time.Sleep(3 * time.Second)
		// log.Println("🔧 Habilitando monitoramento do sistema...")
		orc.EnablePipeline("update_offer")
	}()

	// Mostra métricas a cada 25 segundos
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Println("\n📊 MÉTRICAS ATUAIS:")
				metrics := orc.GetMetrics()
				for name, data := range metrics {
					d := data.(map[string]interface{})
					log.Printf("  📈 %s: Total=%d | Sucesso=%d | Erro=%d",
						name, d["executions_total"], d["executions_success"], d["executions_error"])
				}
				log.Println()
			}
		}
	}()

	// Executa por 2 minutos
	time.Sleep(2 * time.Minute)

	// Encerra graciosamente
	log.Println("🛑 Encerrando orquestrador...")
	cancel()

	if err := orc.Shutdown(5 * time.Second); err != nil {
		log.Printf("Erro no shutdown: %v", err)
	} else {
		log.Println("✅ Orquestrador encerrado com sucesso!")
	}
}

func buildPipeline() func(ctx context.Context) ([]taskflow.Executable, error) {
	client := http.DefaultClient
	return func(ctx context.Context) ([]taskflow.Executable, error) {
		getEvents := taskflow.NewTask("get_events", func(ctx context.Context, _ any) ([]*Event, error) {
			log.Println("🔍 Buscando eventos")
			time.Sleep(2 * time.Second) // Simula um atraso na busca de eventos
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/events", nil)
			if err != nil {
				log.Printf("❌ Erro ao criar requisição: %v", err)
				return nil, err
			}
			res, err := client.Do(req)
			if err != nil {
				log.Printf("❌ Erro ao buscar eventos: %v", err)
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao buscar eventos", res.StatusCode)
				log.Printf("❌ Erro ao buscar eventos: %v", err)
				return nil, err
			}
			log.Println("✅ Eventos obtidos com sucesso")
			// Simula o processamento dos eventos
			return []*Event{
				{ID: "1", Name: "Evento 1", OfferID: "offer-123"},
				{ID: "2", Name: "Evento 2", OfferID: "offer-456"},
				{ID: "3", Name: "Evento 3", OfferID: "offer-789"},
			}, nil
		})

		getCredentials := taskflow.NewTask("get_credentials", func(ctx context.Context, events []*Event) (*EventsAndCredentials, error) {
			log.Println("🔍 Buscando credenciais")
			time.Sleep(2 * time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/credentials", nil)
			if err != nil {
				log.Printf("❌ Erro ao criar requisição: %v", err)
				return nil, err
			}

			res, err := client.Do(req)
			if err != nil {
				log.Printf("❌ Erro ao buscar credenciais: %v", err)
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao buscar credenciais", res.StatusCode)
				log.Printf("❌ Erro ao buscar credenciais: %v", err)
				return nil, err
			}
			log.Println("✅ Credenciais obtidas com sucesso")

			var creds Credentials
			// Simula o preenchimento das credenciais
			creds.Token = "example-token"
			return &EventsAndCredentials{
				Events:      events,
				Credentials: &creds,
			}, nil
		}).After(getEvents)

		sendToChannel := taskflow.NewTask("send_to_channel", func(ctx context.Context, input *EventsAndCredentials) ([]*Event, error) {
			log.Println("📤 Enviando eventos para o canal")
			time.Sleep(2 * time.Second)
			b, err := json.Marshal(input.Events)
			if err != nil {
				log.Printf("❌ Erro ao serializar eventos: %v", err)
				return nil, err
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8080/send", bytes.NewBuffer(b))
			req.Header.Set("Authorization", "Bearer "+input.Credentials.Token)
			res, err := client.Do(req)
			if err != nil {
				log.Printf("❌ Erro ao enviar eventos: %v", err)
				return nil, err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err := fmt.Errorf("status %d ao enviar eventos", res.StatusCode)
				log.Printf("❌ Erro ao enviar eventos: %v", err)
				return nil, err
			}
			log.Println("✅ Eventos enviados com sucesso")
			// Simula o envio bem-sucedido

			return input.Events, nil
		}).After(getCredentials)

		updateOfferGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			log.Println("FanOutTask: Gerando funções de fan-out para atualização de ofertas...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					log.Printf("FanOutTask: Processando atualização de oferta %s...", event.OfferID)

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/offers/%s", event.OfferID), nil)
					resp, err := client.Do(req)
					if err != nil {
						log.Printf("❌ Erro ao atualizar offerta %s: %v", event.OfferID, err)
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d ao atualizar oferta %s", resp.StatusCode, event.ID)
						log.Printf("❌ Erro ao atualizar oferta %s: %v", event.OfferID, err)
						return "", err
					}
					log.Printf("✔️ Oferta %s atualizado com sucesso", event.OfferID)
					return event.OfferID, nil
				})
			}

			return fns, nil
		}

		updateOfferfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				log.Printf("FanOutTask: Resultado recebido: %s", result)
			}
			log.Println("FanOutTask: Consolidando resultados...")
			return "success", nil
		}

		updateOfferfanOutTask := &taskflow.FanOutTask[*Event, string]{
			Name:     "update_offer_fanout",
			Generate: updateOfferGenerateFunc,
			FanIn:    updateOfferfanInFunc,
		}

		updateEventGenerateFunc := func(ctx context.Context, events []*Event) ([]taskflow.TaskFunc[*Event, string], error) {
			log.Println("FanOutTask: Gerando funções de fan-out para atualização de evenos...")
			time.Sleep(2 * time.Second)
			fns := make([]taskflow.TaskFunc[*Event, string], 0, len(events))
			for _, event := range events {
				fns = append(fns, func(ctx context.Context, _ *Event) (string, error) {
					log.Printf("FanOutTask: Processando atualização de evento %s...", event.ID)

					req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("http://localhost:8080/events/%s", event.ID), nil)
					resp, err := client.Do(req)
					if err != nil {
						log.Printf("❌ Erro ao atualizar evento %s: %v", event.ID, err)
						return "", err
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						err := fmt.Errorf("status %d ao atualizar evento %s", resp.StatusCode, event.ID)
						log.Printf("❌ Erro ao atualizar evento %s: %v", event.ID, err)
						return "", err
					}
					log.Printf("✔️ Evento %s atualizado com sucesso", event.ID)
					return event.ID, nil
				})
			}

			return fns, nil
		}

		updateEventfanInFunc := func(ctx context.Context, results []string) (string, error) {
			for _, result := range results {
				log.Printf("FanOutTask: Resultado recebido: %s", result)
			}
			log.Println("FanOutTask: Consolidando resultados...")
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
