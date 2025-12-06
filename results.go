package syncext

import "errors"

var (
	// ErrBufferFull is returned when Add is called and the buffer is full.
	ErrBufferFull = errors.New("results buffer is full")
)

// Results is a thread-safe collection that accumulates values of type T
// from multiple goroutines. Values are collected via Add and retrieved
// by calling Close, which returns all accumulated values.
//
// Results uses a buffered channel internally, so Add operations are
// non-blocking until the buffer is full. It is the caller's responsibility
// to ensure all Add calls complete before calling Close.
type Results[T any] struct {
	channel chan T
}

// NewResults creates a new Results instance with the specified buffer size.
// The size determines how many values can be added without blocking.
// Once the buffer is full, Add will return ErrBufferFull.
func NewResults[T any](size int) *Results[T] {
	return &Results[T]{
		channel: make(chan T, size),
	}
}

// Add adds a value to the results collection. This operation is non-blocking
// and thread-safe. It returns ErrBufferFull if the buffer is full.
//
// Add can be called concurrently from multiple goroutines. The caller must
// ensure all Add calls complete before calling Close.
func (r *Results[T]) Add(result T) error {
	select {
	case r.channel <- result:
		return nil
	default:
		return ErrBufferFull
	}
}

// Close closes the results collection and returns all accumulated values.
// The caller must ensure all Add calls have completed before calling Close.
// Calling Close multiple times will panic.
//
// Close should be called from a single goroutine to avoid race conditions
// when reading the results.
func (r *Results[T]) Close() []T {
	close(r.channel)

	results := make([]T, 0, cap(r.channel))

	for {
		result, ok := <-r.channel
		if !ok {
			break
		}

		results = append(results, result)
	}

	return results
}

// Len returns the number of values currently buffered in the results.
// This is a snapshot and may change immediately after the call if Add
// is being called concurrently.
func (r *Results[T]) Len() int {
	return len(r.channel)
}
