# Orchestrator

A robust Go library for managing and orchestrating multiple task pipelines with concurrent execution, automatic retries, and dynamic control.

## Features

- 🔄 **Periodic Pipeline Execution**: Run pipelines at specified intervals
- 🚀 **Concurrent Processing**: Control maximum concurrency per pipeline
- 🔁 **Automatic Retries**: Configurable retry attempts with delays
- ⏱️ **Timeout Management**: Set execution timeouts for pipelines
- 🎛️ **Dynamic Control**: Enable/disable pipelines at runtime
- 📊 **Success/Error Callbacks**: Handle pipeline results and errors
- 🪵 **Configurable Logging**: Integrate with your preferred logging solution
- 🛡️ **Graceful Shutdown**: Clean shutdown with timeout support

## Installation

```bash
go get github.com/josuedeavila/orchestrator
```

## Quick Start

```go
package main

import (
    "context"
    "time"
    "github.com/josuedeavila/orchestrator"
    "github.com/josuedeavila/taskflow"
)

func main() {
    // Create orchestrator
    orc := orchestrator.NewPipelineOrchestrator()

    // Configure pipeline
    config := &orchestrator.PipelineConfig{
        Name:           "my_pipeline",
        Interval:       30 * time.Second,
        MaxConcurrency: 2,
        MaxRetries:     3,
        Timeout:        60 * time.Second,
        Enabled:        true,
        PipelineBuilder: func(ctx context.Context) ([]taskflow.Executable, error) {
            task := taskflow.NewTask("example", func(ctx context.Context, _ any) (string, error) {
                return "Hello, World!", nil
            })
            return []taskflow.Executable{task}, nil
        },
        OnSuccess: func(ctx context.Context, result any) {
            fmt.Printf("Pipeline succeeded: %v\n", result)
        },
        OnError: func(ctx context.Context, err error) {
            fmt.Printf("Pipeline failed: %v\n", err)
        },
    }

    // Add and start
    orc.AddPipeline(config)
    
    ctx := context.Background()
    orc.Start(ctx)
    
    // Run for some time...
    time.Sleep(5 * time.Minute)
    
    // Graceful shutdown
    orc.Shutdown(10 * time.Second)
}
```

## Pipeline Configuration

### PipelineConfig Options

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `Name` | `string` | Unique pipeline identifier | Required |
| `Interval` | `time.Duration` | Execution interval | Required |
| `MaxConcurrency` | `int` | Maximum concurrent executions | `1` |
| `MaxRetries` | `int` | Maximum retry attempts | `0` |
| `RetryDelay` | `time.Duration` | Delay between retries | `1s` |
| `Timeout` | `time.Duration` | Execution timeout | `30s` |
| `Enabled` | `bool` | Initial enabled state | `false` |
| `PipelineBuilder` | `func` | Function to build pipeline tasks | Required |
| `OnSuccess` | `func` | Success callback | Optional |
| `OnError` | `func` | Error callback | Optional |

## Usage Examples

### Basic Pipeline with HTTP Tasks

```go
func buildHTTPPipeline(client *http.Client) func(context.Context) ([]taskflow.Executable, error) {
    return func(ctx context.Context) ([]taskflow.Executable, error) {
        fetchData := taskflow.NewTask("fetch_data", func(ctx context.Context, _ any) (*Data, error) {
            resp, err := client.Get("https://api.example.com/data")
            if err != nil {
                return nil, err
            }
            defer resp.Body.Close()
            
            var data Data
            json.NewDecoder(resp.Body).Decode(&data)
            return &data, nil
        })

        processData := taskflow.NewTask("process_data", func(ctx context.Context, data *Data) (*ProcessedData, error) {
            // Process the data
            return &ProcessedData{Value: data.RawValue * 2}, nil
        }).After(fetchData)

        return []taskflow.Executable{fetchData, processData}, nil
    }
}
```

### Fan-Out Pattern

```go
func buildFanOutPipeline() func(context.Context) ([]taskflow.Executable, error) {
    return func(ctx context.Context) ([]taskflow.Executable, error) {
        getData := taskflow.NewTask("get_data", func(ctx context.Context, _ any) ([]Item, error) {
            return []Item{{ID: 1}, {ID: 2}, {ID: 3}}, nil
        })

        processFanOut := &taskflow.FanOutTask[Item, string]{
            Name: "process_items",
            Generate: func(ctx context.Context, items []Item) ([]taskflow.TaskFunc[Item, string], error) {
                var fns []taskflow.TaskFunc[Item, string]
                for _, item := range items {
                    fns = append(fns, func(ctx context.Context, item Item) (string, error) {
                        // Process individual item
                        return fmt.Sprintf("processed-%d", item.ID), nil
                    })
                }
                return fns, nil
            },
            FanIn: func(ctx context.Context, results []string) (string, error) {
                return strings.Join(results, ","), nil
            },
        }

        processTask := processFanOut.ToTask().After(getData)
        return []taskflow.Executable{getData, processTask}, nil
    }
}
```

### Multiple Pipelines

```go
configs := []*orchestrator.PipelineConfig{
    {
        Name:            "data_sync",
        Interval:        5 * time.Minute,
        MaxConcurrency:  1,
        MaxRetries:      3,
        Enabled:         true,
        PipelineBuilder: buildDataSyncPipeline(),
    },
    {
        Name:            "health_check",
        Interval:        30 * time.Second,
        MaxConcurrency:  1,
        MaxRetries:      2,
        Enabled:         true,
        PipelineBuilder: buildHealthCheckPipeline(),
    },
    {
        Name:            "cleanup",
        Interval:        1 * time.Hour,
        MaxConcurrency:  1,
        MaxRetries:      1,
        Enabled:         false, // Start disabled
        PipelineBuilder: buildCleanupPipeline(),
    },
}

for _, config := range configs {
    orc.AddPipeline(config)
}
```

## Dynamic Pipeline Control

```go
// Enable/disable pipelines at runtime
orc.EnablePipeline("cleanup")
orc.DisablePipeline("data_sync")

// Remove pipelines
orc.RemovePipeline("old_pipeline")

// List all pipelines
pipelines := orc.ListPipelines()
fmt.Printf("Active pipelines: %v\n", pipelines)
```

## Logging

```go
type MyLogger struct {
    *slog.Logger
}

func (l *MyLogger) Log(args ...any) {
    l.Logger.Info(fmt.Sprint(args...))
}

orc := orchestrator.NewPipelineOrchestrator().WithLogger(&MyLogger{
    Logger: slog.Default(),
})
```

## Error Handling

The orchestrator provides multiple levels of error handling:

1. **Task-level**: Individual tasks can return errors
2. **Pipeline-level**: Entire pipeline execution can fail
3. **Retry mechanism**: Automatic retries with configurable delays
4. **Callbacks**: Custom error handlers via `OnError` callback

```go
config := &orchestrator.PipelineConfig{
    // ... other config
    MaxRetries: 3,
    RetryDelay: 5 * time.Second,
    OnError: func(ctx context.Context, err error) {
        // Log error, send alert, etc.
        logger.Error("Pipeline failed", "error", err)
        alertService.SendAlert("Pipeline failure", err.Error())
    },
}
```

## Graceful Shutdown

```go
// Set up signal handling
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go func() {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    cancel()
}()

// Start orchestrator
orc.Start(ctx)

// Wait for shutdown signal
<-ctx.Done()

// Graceful shutdown with timeout
if err := orc.Shutdown(30 * time.Second); err != nil {
    log.Printf("Shutdown timeout: %v", err)
}
```

## Dependencies

- [taskflow](https://github.com/josuedeavila/taskflow) - Task execution framework

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Examples

Check out the [example](example/) directory for more comprehensive examples including:

- Simple pipeline orchestration
- HTTP-based task pipelines
- Fan-out/fan-in patterns
- Error handling and retries
- Dynamic pipeline control