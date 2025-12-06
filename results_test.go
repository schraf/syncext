package syncext

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResults_NewResults(t *testing.T) {
	t.Run("valid size", func(t *testing.T) {
		r := NewResults[int](10)
		require.NotNil(t, r)
		assert.Equal(t, 0, r.Len())
	})

	t.Run("zero size", func(t *testing.T) {
		r := NewResults[string](0)
		require.NotNil(t, r)
		assert.Equal(t, 0, r.Len())
	})

	t.Run("large size", func(t *testing.T) {
		r := NewResults[int](1000)
		require.NotNil(t, r)
		assert.Equal(t, 0, r.Len())
	})
}

func TestResults_Add(t *testing.T) {
	t.Run("successful add", func(t *testing.T) {
		r := NewResults[int](5)
		err := r.Add(42)
		assert.NoError(t, err)
		assert.Equal(t, 1, r.Len())
	})

	t.Run("multiple adds", func(t *testing.T) {
		r := NewResults[string](5)
		err := r.Add("first")
		require.NoError(t, err)
		assert.Equal(t, 1, r.Len())

		err = r.Add("second")
		require.NoError(t, err)
		assert.Equal(t, 2, r.Len())

		err = r.Add("third")
		require.NoError(t, err)
		assert.Equal(t, 3, r.Len())
	})

	t.Run("add fills buffer", func(t *testing.T) {
		r := NewResults[int](3)
		err := r.Add(1)
		require.NoError(t, err)
		err = r.Add(2)
		require.NoError(t, err)
		err = r.Add(3)
		require.NoError(t, err)
		assert.Equal(t, 3, r.Len())

		// Buffer is now full
		err = r.Add(4)
		assert.Error(t, err)
		assert.Equal(t, ErrBufferFull, err)
		assert.Equal(t, 3, r.Len())
	})

	t.Run("add after close panics", func(t *testing.T) {
		r := NewResults[int](5)
		err := r.Add(1)
		require.NoError(t, err)

		results := r.Close()
		assert.Equal(t, []int{1}, results)

		// Try to add after close - should panic
		assert.Panics(t, func() {
			_ = r.Add(2)
		})
	})
}

func TestResults_Close(t *testing.T) {
	t.Run("close empty results", func(t *testing.T) {
		r := NewResults[int](5)
		results := r.Close()
		assert.Equal(t, []int{}, results)
		assert.Equal(t, 0, r.Len())
	})

	t.Run("close with single value", func(t *testing.T) {
		r := NewResults[string](5)
		err := r.Add("hello")
		require.NoError(t, err)

		results := r.Close()
		assert.Equal(t, []string{"hello"}, results)
	})

	t.Run("close with multiple values", func(t *testing.T) {
		r := NewResults[int](10)
		for i := 1; i <= 5; i++ {
			err := r.Add(i)
			require.NoError(t, err)
		}

		results := r.Close()
		assert.Equal(t, []int{1, 2, 3, 4, 5}, results)
	})

	t.Run("close fills buffer", func(t *testing.T) {
		r := NewResults[int](3)
		err := r.Add(10)
		require.NoError(t, err)
		err = r.Add(20)
		require.NoError(t, err)
		err = r.Add(30)
		require.NoError(t, err)

		results := r.Close()
		assert.Equal(t, []int{10, 20, 30}, results)
	})

	t.Run("close multiple times panics", func(t *testing.T) {
		r := NewResults[int](5)
		err := r.Add(1)
		require.NoError(t, err)
		err = r.Add(2)
		require.NoError(t, err)

		results1 := r.Close()
		assert.Equal(t, []int{1, 2}, results1)

		// Close again - should panic
		assert.Panics(t, func() {
			_ = r.Close()
		})
	})

	t.Run("close preserves order", func(t *testing.T) {
		r := NewResults[int](10)
		expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		for _, v := range expected {
			err := r.Add(v)
			require.NoError(t, err)
		}

		results := r.Close()
		assert.Equal(t, expected, results)
	})
}

func TestResults_Len(t *testing.T) {
	t.Run("empty results", func(t *testing.T) {
		r := NewResults[int](5)
		assert.Equal(t, 0, r.Len())
	})

	t.Run("len increases with adds", func(t *testing.T) {
		r := NewResults[string](5)
		assert.Equal(t, 0, r.Len())

		r.Add("a")
		assert.Equal(t, 1, r.Len())

		r.Add("b")
		assert.Equal(t, 2, r.Len())

		r.Add("c")
		assert.Equal(t, 3, r.Len())
	})

	t.Run("len after close", func(t *testing.T) {
		r := NewResults[int](5)
		r.Add(1)
		r.Add(2)
		assert.Equal(t, 2, r.Len())

		r.Close()
		assert.Equal(t, 0, r.Len())
	})

	t.Run("len reflects buffer state", func(t *testing.T) {
		r := NewResults[int](3)
		r.Add(1)
		assert.Equal(t, 1, r.Len())
		r.Add(2)
		assert.Equal(t, 2, r.Len())
		r.Add(3)
		assert.Equal(t, 3, r.Len())
	})
}

func TestResults_ConcurrentAdd(t *testing.T) {
	t.Run("multiple goroutines adding", func(t *testing.T) {
		r := NewResults[int](100)
		var wg sync.WaitGroup
		count := 50

		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				err := r.Add(val)
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()
		assert.Equal(t, count, r.Len())

		results := r.Close()
		assert.Equal(t, count, len(results))
		// Check that all values are present (order may vary)
		seen := make(map[int]bool)
		for _, v := range results {
			seen[v] = true
		}
		assert.Equal(t, count, len(seen))
	})

	t.Run("concurrent adds with buffer limit", func(t *testing.T) {
		r := NewResults[int](10)
		var wg sync.WaitGroup
		var mu sync.Mutex
		successCount := 0
		fullCount := 0

		// Try to add more than buffer size
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				err := r.Add(val)
				mu.Lock()
				if err == nil {
					successCount++
				} else if err == ErrBufferFull {
					fullCount++
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()
		assert.Equal(t, 10, successCount)
		assert.Equal(t, 10, fullCount)
		assert.Equal(t, 10, r.Len())
	})
}

func TestResults_ConcurrentClose(t *testing.T) {
	t.Run("multiple goroutines calling close panics", func(t *testing.T) {
		r := NewResults[int](10)
		for i := 0; i < 5; i++ {
			r.Add(i)
		}

		var wg sync.WaitGroup
		panicked := make(chan bool, 10)

		// Multiple goroutines trying to close - all but one should panic
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if recover() != nil {
						panicked <- true
					}
				}()
				_ = r.Close()
			}()
		}

		wg.Wait()
		close(panicked)

		// At least 9 should panic (only one can successfully close)
		panicCount := 0
		for range panicked {
			panicCount++
		}
		assert.GreaterOrEqual(t, panicCount, 9)
	})
}

func TestResults_ResultsConcurrentOperations(t *testing.T) {
	t.Run("add and close concurrently", func(t *testing.T) {
		r := NewResults[int](100)
		var wg sync.WaitGroup
		addCount := 50

		// Start adding values
		for i := 0; i < addCount; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				// Add might panic if Close happens first, but that's expected
				defer func() {
					_ = recover()
				}()
				r.Add(val)
			}(i)
		}

		// Wait a bit to let some adds complete
		wg.Wait()

		// Now close after all adds should be done
		results := r.Close()

		// Results should contain all values that were added before close
		assert.GreaterOrEqual(t, len(results), 0)
		assert.LessOrEqual(t, len(results), addCount)
	})

	t.Run("concurrent adds with len checks", func(t *testing.T) {
		r := NewResults[int](50)
		var wg sync.WaitGroup
		count := 30

		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				err := r.Add(val)
				if err == nil {
					// Check len (may change immediately)
					_ = r.Len()
				}
			}(i)
		}

		wg.Wait()
		assert.Equal(t, count, r.Len())
	})
}

func TestResults_EdgeCases(t *testing.T) {
	t.Run("zero buffer size", func(t *testing.T) {
		r := NewResults[int](0)
		err := r.Add(1)
		assert.Error(t, err)
		assert.Equal(t, ErrBufferFull, err)
	})

	t.Run("add after multiple closes panics", func(t *testing.T) {
		r := NewResults[int](5)
		r.Add(1)
		r.Close()

		// Second close should panic
		assert.Panics(t, func() {
			r.Close()
		})

		// Add after close should panic
		assert.Panics(t, func() {
			_ = r.Add(2)
		})
	})

	t.Run("buffer full then close", func(t *testing.T) {
		r := NewResults[int](3)
		r.Add(1)
		r.Add(2)
		r.Add(3)

		// Try to add when full
		err := r.Add(4)
		assert.Error(t, err)
		assert.Equal(t, ErrBufferFull, err)

		// Close should return all 3 values
		results := r.Close()
		assert.Equal(t, []int{1, 2, 3}, results)
	})

	t.Run("generic string type", func(t *testing.T) {
		r := NewResults[string](5)
		r.Add("hello")
		r.Add("world")
		results := r.Close()
		assert.Equal(t, []string{"hello", "world"}, results)
	})

	t.Run("generic struct type", func(t *testing.T) {
		type Point struct {
			X, Y int
		}
		r := NewResults[Point](5)
		r.Add(Point{X: 1, Y: 2})
		r.Add(Point{X: 3, Y: 4})
		results := r.Close()
		assert.Equal(t, []Point{{X: 1, Y: 2}, {X: 3, Y: 4}}, results)
	})
}
