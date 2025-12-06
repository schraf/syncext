package syncext

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestJobError struct {
	Id JobId
}

func (e *TestJobError) Error() string {
	return fmt.Sprintf("job %s error", e.Id.String())
}

func (e *TestJobError) Is(target error) bool {
	t, ok := target.(*TestJobError)
	if !ok {
		return false
	}
	return e.Id == t.Id
}

type TestJob struct {
	Id        JobId
	DidRun    bool
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
}

func NewTestJob(duration time.Duration) TestJob {
	return TestJob{
		Id:       NewJobId(),
		Duration: duration,
	}
}

func (j *TestJob) ScheduleSuccess(system *JobSystem, dependencies []JobId) error {
	return system.ScheduleJob(j.Id, dependencies, func(ctx context.Context) error {
		j.StartTime = time.Now()
		time.Sleep(j.Duration)
		j.EndTime = time.Now()
		j.DidRun = true
		return nil
	})
}

func (j *TestJob) ScheduleFailure(system *JobSystem, dependencies []JobId) error {
	return system.ScheduleJob(j.Id, dependencies, func(ctx context.Context) error {
		j.StartTime = time.Now()
		time.Sleep(j.Duration)
		j.EndTime = time.Now()
		j.DidRun = true
		return &TestJobError{
			Id: j.Id,
		}
	})
}

func TestJobSystem_NoJobs(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)
}

func TestJobSystem_SingleSuccess(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)
	err = job.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)

	assert.True(t, job.DidRun)
}

func TestJobSystem_SingleFailure(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)
	err = job.ScheduleFailure(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.ErrorIs(t, err, &TestJobError{Id: job.Id})

	assert.True(t, job.DidRun)
}

func TestJobSystem_InvalidDependency(t *testing.T) {
	var err error

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)
	invalidJobId := NewJobId()

	err = job.ScheduleSuccess(system, []JobId{invalidJobId})
	assert.ErrorIs(t, err, &JobDependencyError{Dependent: job.Id, Dependency: invalidJobId})
}

func TestJobSystem_ScheduleMultipleTimes(t *testing.T) {
	var err error

	system := NewJobSystem()

	job := NewTestJob(1 * time.Millisecond)

	err = job.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = job.ScheduleSuccess(system, nil)
	assert.ErrorIs(t, err, &JobAlreadyScheduledError{Job: job.Id})
}

func TestJobSystem_DependencyChain(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(500 * time.Millisecond)
	jobB := NewTestJob(500 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, []JobId{jobA.Id})
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.Greater(t, jobB.StartTime, jobA.EndTime)
}

func TestJobSystem_DependencyChainWithFailure(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(500 * time.Millisecond)
	jobB := NewTestJob(500 * time.Millisecond)
	jobC := NewTestJob(500 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleFailure(system, []JobId{jobA.Id})
	assert.NoError(t, err)

	err = jobC.ScheduleSuccess(system, []JobId{jobB.Id})
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.ErrorIs(t, err, &TestJobError{Id: jobB.Id})

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.False(t, jobC.DidRun)
}

func TestJobSystem_Concurrency(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(100 * time.Millisecond)
	jobB := NewTestJob(100 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.Less(t, jobB.StartTime, jobA.EndTime)
	assert.Less(t, jobA.StartTime, jobB.EndTime)
}

func TestJobSystem_MultipleDependents(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(500 * time.Millisecond)
	jobB := NewTestJob(10 * time.Millisecond)
	jobC := NewTestJob(100 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobC.ScheduleSuccess(system, []JobId{jobA.Id, jobB.Id})
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.True(t, jobC.DidRun)
	assert.Greater(t, jobC.StartTime, jobA.EndTime)
	assert.Greater(t, jobC.StartTime, jobB.EndTime)
}

func TestJobSystem_MultipleDependencies(t *testing.T) {
	var err error

	ctx := context.Background()

	system := NewJobSystem()

	jobA := NewTestJob(500 * time.Millisecond)
	jobB := NewTestJob(10 * time.Millisecond)
	jobC := NewTestJob(100 * time.Millisecond)

	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = jobB.ScheduleSuccess(system, []JobId{jobA.Id})
	assert.NoError(t, err)

	err = jobC.ScheduleSuccess(system, []JobId{jobA.Id})
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.True(t, jobC.DidRun)
	assert.Greater(t, jobB.StartTime, jobA.EndTime)
	assert.Greater(t, jobC.StartTime, jobA.EndTime)
	assert.Less(t, jobB.StartTime, jobC.EndTime)
	assert.Less(t, jobC.StartTime, jobB.EndTime)
}

func TestJobSystem_AlreadyRunning(t *testing.T) {
	ctx := context.Background()
	system := NewJobSystem()

	job := NewTestJob(500 * time.Millisecond)
	err := job.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	firstRun := make(chan struct{}, 1)
	go func() {
		err := system.Run(ctx, 1000)
		assert.NoError(t, err)
		firstRun <- struct{}{}
	}()

	time.Sleep(10 * time.Millisecond)

	err = system.Run(ctx, 1000)
	assert.ErrorIs(t, err, &JobSystemAlreadyRunningError{})

	<-firstRun
}

func TestJobSystem_MultipleSequentialRuns(t *testing.T) {
	ctx := context.Background()
	system := NewJobSystem()

	// First run
	jobA := NewTestJob(1 * time.Millisecond)
	err := jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)
	assert.True(t, jobA.DidRun)

	// Second run with new jobs
	jobB := NewTestJob(1 * time.Millisecond)
	err = jobB.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	err = system.Run(ctx, 1000)
	assert.NoError(t, err)
	assert.True(t, jobB.DidRun)
}

func TestJobSystem_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	system := NewJobSystem()

	jobStarted := make(chan bool, 1)
	jobCancelled := make(chan bool, 1)

	job := NewTestJob(500 * time.Millisecond)
	err := system.ScheduleJob(job.Id, nil, func(ctx context.Context) error {
		jobStarted <- true
		select {
		case <-ctx.Done():
			jobCancelled <- true
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})
	assert.NoError(t, err)

	// Start Run() in a goroutine
	runErr := make(chan error, 1)
	go func() {
		runErr <- system.Run(ctx, 1000)
	}()

	// Wait for job to start
	<-jobStarted

	// Cancel context
	cancel()

	// Wait for Run() to complete
	err = <-runErr
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	// Verify job detected cancellation
	select {
	case <-jobCancelled:
		// Correct, job detected cancellation
	case <-time.After(1 * time.Second):
		t.Fatal("Job did not detect context cancellation")
	}
}

func TestJobSystem_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	system := NewJobSystem()

	jobStarted := make(chan bool, 1)

	job := NewTestJob(500 * time.Millisecond)
	err := system.ScheduleJob(job.Id, nil, func(ctx context.Context) error {
		jobStarted <- true
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})
	assert.NoError(t, err)

	// Start Run() in a goroutine
	runErr := make(chan error, 1)
	go func() {
		runErr <- system.Run(ctx, 1000)
	}()

	// Wait for job to start
	<-jobStarted

	// Wait for timeout
	err = <-runErr
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestJobSystem_ScheduleWhileRunning(t *testing.T) {
	var err error

	ctx := context.Background()
	system := NewJobSystem()

	jobA := NewTestJob(200 * time.Millisecond)
	err = jobA.ScheduleSuccess(system, nil)
	assert.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- system.Run(ctx, 1000)
	}()

	// ensure system is running
	time.Sleep(10 * time.Millisecond)

	jobC := NewTestJob(10 * time.Millisecond)

	jobB := NewTestJob(10 * time.Millisecond)
	err = system.ScheduleJob(jobB.Id, nil, func(ctx context.Context) error {
		err := jobC.ScheduleSuccess(system, []JobId{jobA.Id})
		assert.NoError(t, err)

		jobB.StartTime = time.Now()
		time.Sleep(jobB.Duration)
		jobB.EndTime = time.Now()
		jobB.DidRun = true
		return nil
	})
	assert.NoError(t, err)

	err = <-runErr
	assert.NoError(t, err)

	assert.True(t, jobA.DidRun)
	assert.True(t, jobB.DidRun)
	assert.True(t, jobC.DidRun)

	assert.Greater(t, jobC.StartTime, jobA.EndTime)
}

func TestJobSystem_MaxConcurrency(t *testing.T) {
	ctx := context.Background()
	system := NewJobSystem()

	var runningCount atomic.Int32
	var maxRunning atomic.Int32
	var totalRan atomic.Int32

	jobFunc := func(ctx context.Context) error {
		runningCount.Add(1)
		startTime := time.Now()

		for time.Since(startTime) < 100*time.Millisecond {
			current := runningCount.Load()
			currentMax := maxRunning.Load()

			if current > currentMax {
				maxRunning.CompareAndSwap(currentMax, current)
			}
		}

		runningCount.Add(-1)
		totalRan.Add(1)

		return nil
	}

	for i := 0; i < 10; i++ {
		err := system.ScheduleJob(NewJobId(), nil, jobFunc)
		assert.NoError(t, err)
	}

	concurrencyLimit := 2

	err := system.Run(ctx, concurrencyLimit)
	assert.NoError(t, err)

	// Verify that at most 2 jobs ran concurrently
	assert.LessOrEqual(t, maxRunning.Load(), int32(concurrencyLimit))
	assert.Equal(t, int32(concurrencyLimit), maxRunning.Load())

	// Verify that all jobs ran
	assert.Equal(t, int32(10), totalRan.Load())
}
