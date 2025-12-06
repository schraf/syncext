package syncext

import (
	"context"
	"errors"
)

var (
	// ErrInvalidCount is returned when creating a semaphore with an invalid count
	ErrInvalidCount = errors.New("semaphore count must be greater than 0")
	// ErrReleaseExceedsAcquires is returned when Release is called more times than Acquire
	ErrReleaseExceedsAcquires = errors.New("release exceeds acquires: all permits are already available")
)

// Semaphore is a counting semaphore implementation that allows up to N concurrent
// operations. It uses a buffered channel to track available permits.
type Semaphore struct {
	resource chan struct{}
}

// NewSemaphore creates a new semaphore with the specified capacity.
// The count must be greater than 0, otherwise ErrInvalidCount is returned.
func NewSemaphore(count int) (*Semaphore, error) {
	if count <= 0 {
		return nil, ErrInvalidCount
	}

	sem := &Semaphore{
		resource: make(chan struct{}, count),
	}

	// Pre-fill the channel with permits
	for i := 0; i < count; i++ {
		sem.resource <- struct{}{}
	}

	return sem, nil
}

// Acquire blocks until a permit is available or the context is cancelled.
// Returns nil on successful acquisition, or ctx.Err() if the context is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.resource:
		return nil
	}
}

// TryAcquire attempts to acquire a permit without blocking.
// Returns true if a permit was acquired, false otherwise.
// For timeout-based acquisition, use Acquire with a context that has a timeout.
func (s *Semaphore) TryAcquire() (bool, error) {
	select {
	case <-s.resource:
		return true, nil
	default:
		return false, nil
	}
}

// Release returns a permit to the semaphore.
// Returns ErrReleaseExceedsAcquires if all permits are already available
// (i.e., Release was called more times than Acquire).
func (s *Semaphore) Release() error {
	select {
	case s.resource <- struct{}{}:
		return nil
	default:
		// Channel is full, meaning all permits are already available
		return ErrReleaseExceedsAcquires
	}
}

// Available returns the number of permits currently available.
// This is a snapshot and may change immediately after the call.
func (s *Semaphore) Available() int {
	return len(s.resource)
}

// Capacity returns the maximum number of permits this semaphore can hold.
func (s *Semaphore) Capacity() int {
	return cap(s.resource)
}

