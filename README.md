# syncext

A Go package providing synchronization utilities for concurrent programming.

## Features

### Semaphore

A counting semaphore implementation that allows up to N concurrent operations. Supports blocking and non-blocking acquisition with context cancellation.

### Results

A thread-safe collection for accumulating values from multiple goroutines. Values are collected via `Add` and retrieved by calling `Close`.

### JobSystem

A concurrent job execution system that manages jobs with dependencies. Jobs are executed concurrently while respecting their dependency constraints. The system supports concurrency limits and stops execution if any job fails.

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

### JobSystem

```go
import "github.com/schraf/syncext"

// Create a new job system
system := syncext.NewJobSystem()

// Create job identifiers
jobA := syncext.NewJobId()
jobB := syncext.NewJobId()
jobC := syncext.NewJobId()

// Schedule jobs with dependencies
// jobA has no dependencies
err := system.ScheduleJob(jobA, nil, func(ctx context.Context) error {
    // Do work for job A
    return nil
})

// jobB depends on jobA
err = system.ScheduleJob(jobB, []syncext.JobId{jobA}, func(ctx context.Context) error {
    // Do work for job B (runs after jobA completes)
    return nil
})

// jobC depends on both jobA and jobB
err = system.ScheduleJob(jobC, []syncext.JobId{jobA, jobB}, func(ctx context.Context) error {
    // Do work for job C (runs after both jobA and jobB complete)
    return nil
})

// Run all jobs with a concurrency limit of 5
ctx := context.Background()
err = system.Run(ctx, 5)
if err != nil {
    // Handle error (could be from a job or context cancellation)
}
```

**Error Handling:**

- `JobDependencyError`: Returned when scheduling a job with a dependency that doesn't exist
- `JobAlreadyScheduledError`: Returned when attempting to schedule the same job twice
- `JobSystemAlreadyRunningError`: Returned when attempting to run a system that's already running
- Any error returned by a job function will stop execution and be returned by `Run`

## License

See [LICENSE](LICENSE) file for details.
