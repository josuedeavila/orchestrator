package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/josuedeavila/taskflow"
)

// PipelineConfig define a configuração para uma pipeline específica
type PipelineConfig struct {
	Name            string                                                   // Nome identificador da pipeline
	Interval        time.Duration                                            // Intervalo entre execuções
	MaxConcurrency  int                                                      // Máximo de execuções simultâneas desta pipeline
	MaxRetries      int                                                      // Número máximo de tentativas em caso de erro
	RetryDelay      time.Duration                                            // Delay entre tentativas
	Timeout         time.Duration                                            // Timeout para cada execução
	PipelineBuilder func(ctx context.Context) ([]taskflow.Executable, error) // Função que constrói a pipeline
	OnSuccess       func(ctx context.Context, result any)                    // Callback opcional para sucesso
	OnError         func(ctx context.Context, err error)                     // Callback opcional para erro
	Enabled         bool                                                     // Se a pipeline está habilitada
}

// PipelineOrchestrator gerencia múltiplas pipelines executando periodicamente
type PipelineOrchestrator struct {
	configs    map[string]*PipelineConfig
	semaphores map[string]chan struct{}
	tickers    map[string]*time.Ticker
	shutdown   chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	logger     taskflow.Logger
}


// NewPipelineOrchestrator cria uma nova instância do orquestrador
func NewPipelineOrchestrator() *PipelineOrchestrator {
	return &PipelineOrchestrator{
		configs:    make(map[string]*PipelineConfig),
		semaphores: make(map[string]chan struct{}),
		tickers:    make(map[string]*time.Ticker),
		shutdown:   make(chan struct{}),
		logger:     taskflow.NoOpLogger{},
	}
}

// WithLogger define o logger para o orquestrador
func (o *PipelineOrchestrator) WithLogger(logger taskflow.Logger) *PipelineOrchestrator {
	o.logger = logger
	return o
}

// AddPipeline adiciona uma nova pipeline ao orquestrador
func (o *PipelineOrchestrator) AddPipeline(config *PipelineConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if config.Name == "" {
		return fmt.Errorf("pipeline name cannot be empty")
	}

	if config.Interval <= 0 {
		return fmt.Errorf("pipeline interval must be positive")
	}

	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 1
	}

	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}

	if config.RetryDelay <= 0 {
		config.RetryDelay = 1 * time.Second
	}

	if config.PipelineBuilder == nil {
		return fmt.Errorf("pipeline builder function cannot be nil")
	}

	o.configs[config.Name] = config
	o.semaphores[config.Name] = make(chan struct{}, config.MaxConcurrency)

	o.logger.Log(fmt.Sprintf("Pipeline '%s' adicionada com intervalo de %v", config.Name, config.Interval))
	return nil
}

// RemovePipeline remove uma pipeline do orquestrador
func (o *PipelineOrchestrator) RemovePipeline(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if ticker, exists := o.tickers[name]; exists {
		ticker.Stop()
		delete(o.tickers, name)
	}

	delete(o.configs, name)
	delete(o.semaphores, name)

	o.logger.Log(fmt.Sprintf("Pipeline '%s' removida", name))
}

// EnablePipeline habilita uma pipeline
func (o *PipelineOrchestrator) EnablePipeline(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	config, exists := o.configs[name]
	if !exists {
		return fmt.Errorf("pipeline '%s' not found", name)
	}

	config.Enabled = true
	o.logger.Log(fmt.Sprintf("Pipeline '%s' habilitada", name))
	return nil
}

// DisablePipeline desabilita uma pipeline
func (o *PipelineOrchestrator) DisablePipeline(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	config, exists := o.configs[name]
	if !exists {
		return fmt.Errorf("pipeline '%s' not found", name)
	}

	config.Enabled = false
	o.logger.Log(fmt.Sprintf("Pipeline '%s' desabilitada", name))
	return nil
}

// Start inicia o orquestrador e todas as pipelines habilitadas
func (o *PipelineOrchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	for name, config := range o.configs {
		ticker := time.NewTicker(config.Interval)
		o.tickers[name] = ticker

		o.wg.Add(1)
		go o.runPipelineLoop(ctx, name, config, ticker)
	}

	o.logger.Log("Orquestrador iniciado")
	return nil
}

// runPipelineLoop executa o loop principal de uma pipeline
func (o *PipelineOrchestrator) runPipelineLoop(ctx context.Context, name string, config *PipelineConfig, ticker *time.Ticker) {
	defer o.wg.Done()
	defer ticker.Stop()

	o.logger.Log(fmt.Sprintf("Iniciando loop da pipeline '%s'", name))

	// Executa imediatamente na primeira vez
	if config.Enabled {
		o.schedulePipelineExecution(ctx, name, config)
	}
	for {
		select {
		case <-ctx.Done():
			o.logger.Log(fmt.Sprintf("Pipeline '%s' encerrada por contexto", name))
			return
		case <-o.shutdown:
			o.logger.Log(fmt.Sprintf("Pipeline '%s' encerrada por shutdown", name))
			return
		case <-ticker.C:
			if !config.Enabled {
				continue
			}
			o.schedulePipelineExecution(ctx, name, config)
		}
	}
}

// schedulePipelineExecution agenda a execução de uma pipeline
func (o *PipelineOrchestrator) schedulePipelineExecution(ctx context.Context, name string, config *PipelineConfig) {
	select {
	case o.semaphores[name] <- struct{}{}:
		go func() {
			defer func() { <-o.semaphores[name] }()
			o.executePipeline(ctx, name, config)
		}()
	default:
		o.logger.Log(fmt.Sprintf("Pipeline '%s' pulada - limite de concorrência atingido", name))
	}
}

// executePipeline executa uma pipeline com retry e timeout
func (o *PipelineOrchestrator) executePipeline(ctx context.Context, name string, config *PipelineConfig) {

	var lastErr error
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		pipelineCtx, cancel := context.WithTimeout(ctx, config.Timeout)

		result, err := o.runSinglePipeline(pipelineCtx, name, config)
		cancel()

		if err == nil {
			if config.OnSuccess != nil {
				config.OnSuccess(ctx, result)
			}

			o.logger.Log(fmt.Sprintf("Pipeline '%s' executada com sucesso (tentativa %d/%d)", name, attempt+1, config.MaxRetries+1))
			return
		}

		lastErr = err
		o.logger.Log(fmt.Sprintf("Pipeline '%s' falhou na tentativa %d/%d: %v", name, attempt+1, config.MaxRetries+1, err))

		if attempt < config.MaxRetries {
			select {
			case <-ctx.Done():
				return
			case <-time.After(config.RetryDelay):
				// Continua para a próxima tentativa
			}
		}
	}

	if config.OnError != nil {
		config.OnError(ctx, lastErr)
	}

	o.logger.Log(fmt.Sprintf("Pipeline '%s' falhou após todas as tentativas: %v", name, lastErr))
}

// runSinglePipeline executa uma única instância da pipeline
func (o *PipelineOrchestrator) runSinglePipeline(ctx context.Context, name string, config *PipelineConfig) (any, error) {
	o.logger.Log(fmt.Sprintf("Executando pipeline '%s'", name))

	tasks, err := config.PipelineBuilder(ctx)
	if err != nil {
		return nil, fmt.Errorf("erro ao construir pipeline '%s': %w", name, err)
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("pipeline '%s' não possui tarefas", name)
	}

	runner := taskflow.NewRunner()
	runner.Add(tasks...)

	err = runner.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("erro na execução da pipeline '%s': %w", name, err)
	}

	// Retorna o resultado da última tarefa
	if len(tasks) > 0 {
		t := tasks[len(tasks)-1]
		return t.GetResult(), nil
	}

	return nil, nil
}

// GetPipelineStatus retorna o status de uma pipeline específica
func (o *PipelineOrchestrator) GetPipelineStatus(name string) (map[string]interface{}, error) {
	o.mu.RLock()
	config, exists := o.configs[name]
	o.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("pipeline '%s' not found", name)
	}

	return map[string]interface{}{
		"name":            config.Name,
		"enabled":         config.Enabled,
		"interval":        config.Interval,
		"max_concurrency": config.MaxConcurrency,
		"max_retries":     config.MaxRetries,
	}, nil
}

// Shutdown encerra graciosamente o orquestrador
func (o *PipelineOrchestrator) Shutdown(timeout time.Duration) error {
	o.logger.Log("Iniciando shutdown do orquestrador...")

	close(o.shutdown)

	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		o.logger.Log("Orquestrador encerrado com sucesso")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout no shutdown do orquestrador")
	}
}

// ListPipelines retorna a lista de pipelines configuradas
func (o *PipelineOrchestrator) ListPipelines() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var names []string
	for name := range o.configs {
		names = append(names, name)
	}
	return names
}
