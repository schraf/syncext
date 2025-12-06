package syncext

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSemaphore(t *testing.T) {
	t.Run("valid count", func(t *testing.T) {
		sem, err := NewSemaphore(5)
		require.NoError(t, err)
		require.NotNil(t, sem)
		assert.Equal(t, 5, sem.Capacity())
		assert.Equal(t, 5, sem.Available())
	})

	t.Run("zero count", func(t *testing.T) {
		sem, err := NewSemaphore(0)
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidCount, err)
		assert.Nil(t, sem)
	})

	t.Run("negative count", func(t *testing.T) {
		sem, err := NewSemaphore(-1)
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidCount, err)
		assert.Nil(t, sem)
	})
}

func TestAcquire(t *testing.T) {
	t.Run("successful acquire", func(t *testing.T) {
		sem, err := NewSemaphore(2)
		require.NoError(t, err)

		err = sem.Acquire(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, sem.Available())
	})

	t.Run("context cancellation", func(t *testing.T) {
		sem, err := NewSemaphore(1)
		require.NoError(t, err)

		// Acquire the only permit
		err = sem.Acquire(context.Background())
		require.NoError(t, err)

		// Try to acquire with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = sem.Acquire(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("context timeout", func(t *testing.T) {
		sem, err := NewSemaphore(1)
		require.NoError(t, err)

		// Acquire the only permit
		err = sem.Acquire(context.Background())
		require.NoError(t, err)

		// Try to acquire with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err = sem.Acquire(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("concurrent acquires", func(t *testing.T) {
		sem, err := NewSemaphore(3)
		require.NoError(t, err)

		var wg sync.WaitGroup
		acquired := make(chan bool, 10)

		// Try to acquire 10 permits with only 3 available
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sem.Acquire(context.Background())
				if err == nil {
					acquired <- true
					time.Sleep(10 * time.Millisecond)
					sem.Release()
				}
			}()
		}

		wg.Wait()
		close(acquired)

		count := 0
		for range acquired {
			count++
		}

		// All 10 should eventually acquire (3 at a time)
		assert.Equal(t, 10, count)
		assert.Equal(t, 3, sem.Available())
	})
}

func TestTryAcquire(t *testing.T) {
	t.Run("non-blocking success", func(t *testing.T) {
		sem, err := NewSemaphore(2)
		require.NoError(t, err)

		acquired, err := sem.TryAcquire()
		assert.True(t, acquired)
		assert.NoError(t, err)
		assert.Equal(t, 1, sem.Available())
	})

	t.Run("non-blocking failure", func(t *testing.T) {
		sem, err := NewSemaphore(1)
		require.NoError(t, err)

		// Acquire the only permit
		err = sem.Acquire(context.Background())
		require.NoError(t, err)

		acquired, err := sem.TryAcquire()
		assert.False(t, acquired)
		assert.NoError(t, err)
	})

	t.Run("multiple try acquires", func(t *testing.T) {
		sem, err := NewSemaphore(3)
		require.NoError(t, err)

		acquired, err := sem.TryAcquire()
		assert.True(t, acquired)
		assert.NoError(t, err)
		assert.Equal(t, 2, sem.Available())

		acquired, err = sem.TryAcquire()
		assert.True(t, acquired)
		assert.NoError(t, err)
		assert.Equal(t, 1, sem.Available())

		acquired, err = sem.TryAcquire()
		assert.True(t, acquired)
		assert.NoError(t, err)
		assert.Equal(t, 0, sem.Available())

		// No more permits available
		acquired, err = sem.TryAcquire()
		assert.False(t, acquired)
		assert.NoError(t, err)
	})
}

func TestAcquireWithTimeout(t *testing.T) {
	t.Run("timeout success", func(t *testing.T) {
		sem, err := NewSemaphore(2)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err = sem.Acquire(ctx)
		assert.NoError(t, err)
	})

	t.Run("timeout expiration", func(t *testing.T) {
		sem, err := NewSemaphore(1)
		require.NoError(t, err)

		// Acquire the only permit
		err = sem.Acquire(context.Background())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err = sem.Acquire(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("timeout with release", func(t *testing.T) {
		sem, err := NewSemaphore(1)
		require.NoError(t, err)

		// Acquire the only permit
		err = sem.Acquire(context.Background())
		require.NoError(t, err)

		// Release in a goroutine after a short delay
		go func() {
			time.Sleep(20 * time.Millisecond)
			sem.Release()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err = sem.Acquire(ctx)
		assert.NoError(t, err)
	})
}

func TestRelease(t *testing.T) {
	t.Run("successful release", func(t *testing.T) {
		sem, err := NewSemaphore(2)
		require.NoError(t, err)

		err = sem.Acquire(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, sem.Available())

		err = sem.Release()
		assert.NoError(t, err)
		assert.Equal(t, 2, sem.Available())
	})

	t.Run("release exceeds acquires", func(t *testing.T) {
		sem, err := NewSemaphore(2)
		require.NoError(t, err)

		// All permits are already available
		assert.Equal(t, 2, sem.Available())

		err = sem.Release()
		assert.Error(t, err)
		assert.Equal(t, ErrReleaseExceedsAcquires, err)
	})

	t.Run("release after multiple acquires", func(t *testing.T) {
		sem, err := NewSemaphore(3)
		require.NoError(t, err)

		// Acquire all permits
		for i := 0; i < 3; i++ {
			err = sem.Acquire(context.Background())
			require.NoError(t, err)
		}
		assert.Equal(t, 0, sem.Available())

		// Release all permits
		for i := 0; i < 3; i++ {
			err = sem.Release()
			assert.NoError(t, err)
		}
		assert.Equal(t, 3, sem.Available())

		// Try to release one more
		err = sem.Release()
		assert.Error(t, err)
		assert.Equal(t, ErrReleaseExceedsAcquires, err)
	})
}

func TestAvailable(t *testing.T) {
	sem, err := NewSemaphore(5)
	require.NoError(t, err)

	assert.Equal(t, 5, sem.Available())

	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, sem.Available())

	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, sem.Available())

	err = sem.Release()
	require.NoError(t, err)
	assert.Equal(t, 4, sem.Available())
}

func TestCapacity(t *testing.T) {
	sem, err := NewSemaphore(7)
	require.NoError(t, err)

	assert.Equal(t, 7, sem.Capacity())

	// Capacity should not change after acquires/releases
	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 7, sem.Capacity())

	err = sem.Release()
	require.NoError(t, err)
	assert.Equal(t, 7, sem.Capacity())
}

func TestConcurrentOperations(t *testing.T) {
	sem, err := NewSemaphore(5)
	require.NoError(t, err)

	var wg sync.WaitGroup
	iterations := 100
	goroutines := 10

	// Each goroutine will acquire and release multiple times
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := sem.Acquire(context.Background())
				require.NoError(t, err)
				time.Sleep(time.Microsecond) // Small delay to increase chance of contention
				err = sem.Release()
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	// All permits should be available at the end
	assert.Equal(t, 5, sem.Available())
}

func TestConcurrentTryAcquire(t *testing.T) {
	sem, err := NewSemaphore(3)
	require.NoError(t, err)

	var wg sync.WaitGroup
	acquired := make(chan bool, 20)

	// Multiple goroutines trying to acquire with retries
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Keep trying until we acquire
			for {
				got, err := sem.TryAcquire()
				if err == nil && got {
					acquired <- true
					time.Sleep(10 * time.Millisecond)
					sem.Release()
					return
				}
				// Not acquired, retry after a short delay
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(acquired)

	count := 0
	for range acquired {
		count++
	}
	// All should eventually acquire
	assert.Equal(t, 20, count)
}

func TestAcquireReleasePattern(t *testing.T) {
	sem, err := NewSemaphore(2)
	require.NoError(t, err)

	// Pattern: acquire, release, acquire, release
	for i := 0; i < 10; i++ {
		err = sem.Acquire(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, sem.Available())

		err = sem.Release()
		require.NoError(t, err)
		assert.Equal(t, 2, sem.Available())
	}
}

func TestMultipleAcquiresBeforeRelease(t *testing.T) {
	sem, err := NewSemaphore(3)
	require.NoError(t, err)

	// Acquire multiple permits
	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, sem.Available())

	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, sem.Available())

	err = sem.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, sem.Available())

	// Release all
	err = sem.Release()
	require.NoError(t, err)
	assert.Equal(t, 1, sem.Available())

	err = sem.Release()
	require.NoError(t, err)
	assert.Equal(t, 2, sem.Available())

	err = sem.Release()
	require.NoError(t, err)
	assert.Equal(t, 3, sem.Available())
}
