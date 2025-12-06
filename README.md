# syncext

A Go package providing synchronization utilities for concurrent programming.

## Features

### Semaphore

A counting semaphore implementation that allows up to N concurrent operations. Supports blocking and non-blocking acquisition with context cancellation.

### Results

A thread-safe collection for accumulating values from multiple goroutines. Values are collected via `Add` and retrieved by calling `Close`.

## Installation

```bash
go get github.com/schraf/syncext
```

## Usage

### Semaphore

```go
import "github.com/schraf/syncext"

// Create a semaphore with 3 permits
sem, err := syncext.NewSemaphore(3)
if err != nil {
    // handle error
}

// Acquire a permit (blocking)
err = sem.Acquire(ctx)
if err != nil {
    // handle error
}
defer sem.Release()

// Or try to acquire without blocking
acquired, err := sem.TryAcquire()
if acquired {
    defer sem.Release()
}
```

### Results

```go
import "github.com/schraf/syncext"

// Create a results collection with buffer size 10
results := syncext.NewResults[int](10)

// Add values from multiple goroutines
go func() {
    results.Add(1)
}()
go func() {
    results.Add(2)
}()

// Ensure all Add calls complete, then close
allResults := results.Close()
```

## License

See [LICENSE](LICENSE) file for details.
