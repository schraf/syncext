package syncext

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

//--=========================================================--
//--== Job States
//--=========================================================--

// Job state constants represent the current state of a job in the system.
const (
	waiting  int32 = 0 // Job is waiting to be executed
	blocked  int32 = 1 // Job is blocked waiting for dependency checks
	running  int32 = 2 // Job is currently being executed
	finished int32 = 3 // Job has completed execution
)

//--=========================================================--
//--== Job Function
//--=========================================================--

// JobFunc is the function type that represents a job's work.
// It receives a context and returns an error if the job fails.
type JobFunc func(ctx context.Context) error

//--=========================================================--
//--== Job Identifier
//--=========================================================--

// JobId is a unique identifier for a job in the system.
// It wraps a UUID to provide type-safe job identification.
type JobId uuid.UUID

// NewJobId generates a new unique job identifier.
func NewJobId() JobId {
	return JobId(uuid.New())
}

// String returns the string representation of the job identifier.
func (j JobId) String() string {
	return uuid.UUID(j).String()
}

//--=========================================================--
//--== Errors
//--=========================================================--

// JobDependencyError is returned when a job is scheduled with a dependency
// on a job that does not exist in the system.
type JobDependencyError struct {
	Dependent  JobId // The job that has the invalid dependency
	Dependency JobId // The job that was referenced but does not exist
}

// Error returns a string describing the dependency error.
func (e *JobDependencyError) Error() string {
	return fmt.Sprintf("job %s depends on unknown job %s", e.Dependent.String(), e.Dependency.String())
}

// Is implements error comparison for JobDependencyError.
// It matches if target is a JobDependencyError and all fields match exactly.
func (e *JobDependencyError) Is(target error) bool {
	t, ok := target.(*JobDependencyError)
	if !ok {
		return false
	}

	return e.Dependent == t.Dependent && e.Dependency == t.Dependency
}

// JobAlreadyScheduledError is returned when attempting to schedule a job
// that has already been scheduled in the system.
type JobAlreadyScheduledError struct {
	Job JobId // The job identifier that was already scheduled
}

// Error returns a string describing that the job was already scheduled.
func (e *JobAlreadyScheduledError) Error() string {
	return fmt.Sprintf("job %s has already been scheduled", e.Job)
}

// Is implements error comparison for JobAlreadyScheduledError.
// It matches if target is a JobAlreadyScheduledError and the Job field matches exactly.
func (e *JobAlreadyScheduledError) Is(target error) bool {
	t, ok := target.(*JobAlreadyScheduledError)
	if !ok {
		return false
	}

	return e.Job == t.Job
}

// JobSystemAlreadyRunningError is returned when attempting to run a job system
// that is already running.
type JobSystemAlreadyRunningError struct{}

// Error returns a string indicating the job system is already running.
func (e *JobSystemAlreadyRunningError) Error() string {
	return "job system already running"
}

// Is implements error comparison for JobSystemAlreadyRunningError.
// Since this error has no fields, it matches any JobSystemAlreadyRunningError error.
func (e *JobSystemAlreadyRunningError) Is(target error) bool {
	_, ok := target.(*JobSystemAlreadyRunningError)
	return ok
}

//--=========================================================--
//--== Job System
//--=========================================================--

// job represents an individual job in the system with its state, dependencies, and function.
type job struct {
	state        atomic.Int32
	dependencies []JobId
	function     JobFunc
}

// JobSystem manages a collection of jobs and executes them concurrently
// while respecting their dependencies. Jobs are executed only after all
// their dependencies have completed successfully.
type JobSystem struct {
	jobs    map[JobId]*job
	lock    sync.RWMutex
	running atomic.Bool
}

// NewJobSystem creates and returns a new JobSystem instance.
func NewJobSystem() *JobSystem {
	return &JobSystem{
		jobs: make(map[JobId]*job),
	}
}

// ScheduleJob adds a new job to the system with the given identifier,
// dependencies, and function. The job will not be executed until all
// its dependencies have completed. Returns an error if:
//   - Any dependency does not exist in the system (JobDependencyError)
//   - The job has already been scheduled (JobAlreadyScheduledError)
func (s *JobSystem) ScheduleJob(id JobId, dependencies []JobId, function JobFunc) error {
	if err := s.validateDependencies(id, dependencies); err != nil {
		return err
	}

	if err := s.insertJob(id, dependencies, function); err != nil {
		return err
	}

	return nil
}

// Run executes all scheduled jobs concurrently, respecting their dependencies.
// Jobs are executed as soon as their dependencies are satisfied. The method
// blocks until all jobs have completed or until one job returns an error.
// If any job fails, execution stops and the error is returned. The
// concurrencyLimit specifies the maximum number of jobs that will ever be
// running at the same time. It must be greater than 0.
// Returns JobSystemAlreadyRunningError if the system is already running.
func (s *JobSystem) Run(ctx context.Context, concurrencyLimit int) error {
	if !s.running.CompareAndSwap(false, true) {
		return &JobSystemAlreadyRunningError{}
	}
	defer s.running.Store(false)

	if concurrencyLimit <= 0 {
		return fmt.Errorf("concurrency limit must be greater than 0, got %d", concurrencyLimit)
	}

	var workers sync.WaitGroup
	var startReadyJobs func()
	var err atomic.Value

	sem, semErr := NewSemaphore(concurrencyLimit)
	if semErr != nil {
		return semErr
	}

	startReadyJobs = func() {
		for readyJob := s.acquireJob(); readyJob != nil; readyJob = s.acquireJob() {
			workers.Add(1)

			go func(work *job) {
				defer workers.Done()

				if semError := sem.Acquire(ctx); semError != nil {
					err.CompareAndSwap(nil, semError)
					if err.Load() == nil {
						startReadyJobs()
					}
					return
				}
				defer sem.Release()

				work.state.Store(running)

				if jobError := work.function(ctx); jobError != nil {
					err.CompareAndSwap(nil, jobError)
				}

				work.state.Store(finished)

				if err.Load() == nil {
					startReadyJobs()
				}
			}(readyJob)

			if err.Load() != nil {
				break
			}
		}
	}

	startReadyJobs()
	workers.Wait()

	if err := err.Load(); err != nil {
		return err.(error)
	}

	return nil
}

// validateDependencies checks that all specified dependencies exist in the system.
func (s *JobSystem) validateDependencies(dependent JobId, dependencies []JobId) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, dependency := range dependencies {
		if _, exists := s.jobs[dependency]; !exists {
			return &JobDependencyError{
				Dependent:  dependent,
				Dependency: dependency,
			}
		}
	}

	return nil
}

// insertJob adds a new job to the system, ensuring it hasn't been scheduled before.
func (s *JobSystem) insertJob(id JobId, dependencies []JobId, function JobFunc) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.jobs[id]; exists {
		return &JobAlreadyScheduledError{
			Job: id,
		}
	}

	s.jobs[id] = &job{
		dependencies: dependencies,
		function:     function,
	}

	return nil
}

// acquireJob finds and returns a job that is ready to run (all dependencies satisfied).
// Returns nil if no ready job is available.
func (s *JobSystem) acquireJob() *job {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, job := range s.jobs {
		if job.state.CompareAndSwap(waiting, blocked) {
			ready := true

			for _, dependency := range job.dependencies {
				dependent := s.jobs[dependency]

				if dependent.state.Load() != finished {
					ready = false
					break
				}
			}

			if ready {
				return job
			}

			job.state.Store(waiting)
		}
	}

	return nil
}
