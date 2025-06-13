package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/josuedeavila/taskflow"
)

// PipelineConfig defines the configuration for a specific pipeline
type PipelineConfig struct {
	Name            string                                                   // Pipeline identifier name
	Interval        time.Duration                                            // Interval between executions
	MaxConcurrency  int                                                      // Maximum simultaneous executions of this pipeline
	MaxRetries      int                                                      // Maximum number of attempts on error
	RetryDelay      time.Duration                                            // Delay between attempts
	Timeout         time.Duration                                            // Timeout for each execution
	PipelineBuilder func(ctx context.Context) ([]taskflow.Executable, error) // Function that builds the pipeline
	OnSuccess       func(ctx context.Context, result any)                    // Optional callback for success
	OnError         func(ctx context.Context, err error)                     // Optional callback for error
	Enabled         bool                                                     // Whether the pipeline is enabled
}

// PipelineOrchestrator manages multiple pipelines executing periodically
type PipelineOrchestrator struct {
	configs    map[string]*PipelineConfig
	semaphores map[string]chan struct{}
	tickers    map[string]*time.Ticker
	shutdown   chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	logger     taskflow.Logger
}

// NewPipelineOrchestrator creates a new instance of the orchestrator
func NewPipelineOrchestrator() *PipelineOrchestrator {
	return &PipelineOrchestrator{
		configs:    make(map[string]*PipelineConfig),
		semaphores: make(map[string]chan struct{}),
		tickers:    make(map[string]*time.Ticker),
		shutdown:   make(chan struct{}),
		logger:     taskflow.NoOpLogger{},
	}
}

// WithLogger sets the logger for the orchestrator
func (o *PipelineOrchestrator) WithLogger(logger taskflow.Logger) *PipelineOrchestrator {
	o.logger = logger
	return o
}

// AddPipeline adds a new pipeline to the orchestrator
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

	o.logger.Log(fmt.Sprintf("Pipeline '%s' added with interval of %v", config.Name, config.Interval))
	return nil
}

// RemovePipeline removes a pipeline from the orchestrator
func (o *PipelineOrchestrator) RemovePipeline(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if ticker, exists := o.tickers[name]; exists {
		ticker.Stop()
		delete(o.tickers, name)
	}

	delete(o.configs, name)
	delete(o.semaphores, name)

	o.logger.Log(fmt.Sprintf("Pipeline '%s' removed", name))
}

// EnablePipeline enables a pipeline
func (o *PipelineOrchestrator) EnablePipeline(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	config, exists := o.configs[name]
	if !exists {
		return fmt.Errorf("pipeline '%s' not found", name)
	}

	config.Enabled = true
	o.logger.Log(fmt.Sprintf("Pipeline '%s' enabled", name))
	return nil
}

// DisablePipeline disables a pipeline
func (o *PipelineOrchestrator) DisablePipeline(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	config, exists := o.configs[name]
	if !exists {
		return fmt.Errorf("pipeline '%s' not found", name)
	}

	config.Enabled = false
	o.logger.Log(fmt.Sprintf("Pipeline '%s' disabled", name))
	return nil
}

// Start starts the orchestrator and all enabled pipelines
func (o *PipelineOrchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	for name, config := range o.configs {
		ticker := time.NewTicker(config.Interval)
		o.tickers[name] = ticker

		o.wg.Add(1)
		go o.runPipelineLoop(ctx, name, config, ticker)
	}

	o.logger.Log("Orchestrator started")
	return nil
}

// runPipelineLoop executes the main loop of a pipeline
func (o *PipelineOrchestrator) runPipelineLoop(ctx context.Context, name string, config *PipelineConfig, ticker *time.Ticker) {
	defer o.wg.Done()
	defer ticker.Stop()

	o.logger.Log(fmt.Sprintf("Starting loop for pipeline '%s'", name))

	// Execute immediately on first run
	if config.Enabled {
		o.schedulePipelineExecution(ctx, name, config)
	}
	for {
		select {
		case <-ctx.Done():
			o.logger.Log(fmt.Sprintf("Pipeline '%s' terminated by context", name))
			return
		case <-o.shutdown:
			o.logger.Log(fmt.Sprintf("Pipeline '%s' terminated by shutdown", name))
			return
		case <-ticker.C:
			if !config.Enabled {
				continue
			}
			o.schedulePipelineExecution(ctx, name, config)
		}
	}
}

// schedulePipelineExecution schedules the execution of a pipeline
func (o *PipelineOrchestrator) schedulePipelineExecution(ctx context.Context, name string, config *PipelineConfig) {
	select {
	case o.semaphores[name] <- struct{}{}:
		go func() {
			defer func() { <-o.semaphores[name] }()
			o.executePipeline(ctx, name, config)
		}()
	default:
		o.logger.Log(fmt.Sprintf("Pipeline '%s' skipped - concurrency limit reached", name))
	}
}

// executePipeline executes a pipeline with retry and timeout
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

			o.logger.Log(fmt.Sprintf("Pipeline '%s' executed successfully (attempt %d/%d)", name, attempt+1, config.MaxRetries+1))
			return
		}

		lastErr = err
		o.logger.Log(fmt.Sprintf("Pipeline '%s' failed on attempt %d/%d: %v", name, attempt+1, config.MaxRetries+1, err))

		if attempt < config.MaxRetries {
			select {
			case <-ctx.Done():
				return
			case <-time.After(config.RetryDelay):
				// Continue to next attempt
			}
		}
	}

	if config.OnError != nil {
		config.OnError(ctx, lastErr)
	}

	o.logger.Log(fmt.Sprintf("Pipeline '%s' failed after all attempts: %v", name, lastErr))
}

// runSinglePipeline executes a single instance of the pipeline
func (o *PipelineOrchestrator) runSinglePipeline(ctx context.Context, name string, config *PipelineConfig) (any, error) {
	o.logger.Log(fmt.Sprintf("Executing pipeline '%s'", name))

	tasks, err := config.PipelineBuilder(ctx)
	if err != nil {
		return nil, fmt.Errorf("error building pipeline '%s': %w", name, err)
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("pipeline '%s' has no tasks", name)
	}

	runner := taskflow.NewRunner()
	runner.Add(tasks...)

	err = runner.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("error executing pipeline '%s': %w", name, err)
	}

	// Return the result of the last task
	if len(tasks) > 0 {
		t := tasks[len(tasks)-1]
		return t.GetResult(), nil
	}

	return nil, nil
}

// Shutdown gracefully shuts down the orchestrator
func (o *PipelineOrchestrator) Shutdown(timeout time.Duration) error {
	o.logger.Log("Starting orchestrator shutdown...")

	close(o.shutdown)

	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		o.logger.Log("Orchestrator shutdown completed successfully")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("orchestrator shutdown timeout")
	}
}

// ListPipelines returns the list of configured pipelines
func (o *PipelineOrchestrator) ListPipelines() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var names []string
	for name := range o.configs {
		names = append(names, name)
	}
	return names
}
