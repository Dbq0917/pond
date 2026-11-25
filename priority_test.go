package pond

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alitto/pond/v2/internal/assert"
)

func TestPriorityQueueStrictOrderAndFIFO(t *testing.T) {
	queue := newPriorityQueue(0)

	queue.Push("low-1", PriorityLow)
	queue.Push("normal-1", PriorityNormal)
	queue.Push("high-1", PriorityHigh)
	queue.Push("high-2", PriorityHigh)
	queue.Push("low-2", PriorityLow)
	queue.Push("normal-2", PriorityNormal)
	queue.Push("high-3", PriorityHigh)

	expected := []any{
		"high-1", "high-2", "high-3",
		"normal-1", "normal-2",
		"low-1", "low-2",
	}

	actual := make([]any, 0, len(expected))
	for queue.Len() > 0 {
		task, ok := queue.Pop()
		assert.True(t, ok)
		actual = append(actual, task)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("priority order mismatch:\nexpected %v\nactual   %v", expected, actual)
	}

	if _, ok := queue.Pop(); ok {
		t.Fatal("expected Pop on an empty queue to report false")
	}
}

func TestPriorityQueueFairnessBoundsOvertaking(t *testing.T) {
	const limit = 8
	queue := newPriorityQueue(limit)

	queue.Push("low", PriorityLow)

	popsUntilLow := 0
	for pops := 0; pops < 100; pops++ {
		queue.Push(fmt.Sprintf("high-%d", pops), PriorityHigh)

		task, ok := queue.Pop()
		if !ok {
			t.Fatal("unexpected empty queue")
		}

		popsUntilLow++
		if task == "low" {
			break
		}
	}

	if popsUntilLow > limit+1 {
		t.Fatalf("low priority task starved for %d pops, want at most %d", popsUntilLow, limit+1)
	}
}

func TestPriorityQueueClampsUnknownPriorities(t *testing.T) {
	queue := newPriorityQueue(0)

	queue.Push("very-high-1", Priority(1000))
	queue.Push("very-low-1", Priority(-1000))
	queue.Push("normal", Priority(0))
	queue.Push("very-high-2", Priority(999))

	expected := []any{"very-high-1", "very-high-2", "normal", "very-low-1"}
	actual := make([]any, 0, 4)

	for queue.Len() > 0 {
		task, _ := queue.Pop()
		actual = append(actual, task)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("clamped priority order mismatch: %v", actual)
	}
}

type gatedPool struct {
	pool      Pool
	releaseCh chan struct{}
	started   chan struct{}
	once      sync.Once
}

func newGatedSingleWorkerPool(t *testing.T, options ...Option) *gatedPool {
	t.Helper()

	pool := NewPool(1, options...)
	gated := &gatedPool{
		pool:      pool,
		releaseCh: make(chan struct{}),
		started:   make(chan struct{}),
	}

	t.Cleanup(func() {
		gated.release()
		pool.StopAndWait()
	})

	if err := pool.Go(func() {
		close(gated.started)
		<-gated.releaseCh
	}); err != nil {
		t.Fatalf("failed to occupy worker: %v", err)
	}

	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}

	return gated
}

func (g *gatedPool) release() {
	g.once.Do(func() {
		close(g.releaseCh)
	})
}

// Mirrors the reported incident: a batch of slow background tasks is queued
// first and an urgent task is submitted afterwards. The urgent task must jump
// the queue and finish before the slow batch, while the batch keeps FIFO order.
func TestPoolPriorityUrgentJumpsQueueBeforeSlowBatch(t *testing.T) {
	gated := newGatedSingleWorkerPool(t)
	pool := gated.pool

	var mu sync.Mutex
	order := make([]string, 0, 9)
	finishedAt := make(map[string]time.Time)

	record := func(name string, slow bool) {
		if slow {
			time.Sleep(2 * time.Millisecond)
		}

		mu.Lock()
		order = append(order, name)
		finishedAt[name] = time.Now()
		mu.Unlock()
	}

	const batchSize = 8
	for i := 0; i < batchSize; i++ {
		name := fmt.Sprintf("low-%02d", i)
		if err := pool.GoWithPriority(PriorityLow, func() {
			record(name, true)
		}); err != nil {
			t.Fatalf("submit slow task %s: %v", name, err)
		}
	}

	urgentDone := make(chan struct{})
	submittedAt := time.Now()
	urgentFuture := pool.SubmitWithPriority(PriorityUrgent, func() {
		record("urgent", false)
		close(urgentDone)
	})
	if urgentFuture == nil {
		t.Fatal("expected a future for urgent task")
	}

	gated.release()

	select {
	case <-urgentDone:
	case <-time.After(10 * time.Second):
		t.Fatal("urgent task did not complete in time")
	}

	urgentLatency := time.Since(submittedAt)
	t.Logf("urgent task completed in %v", urgentLatency)

	mu.Lock()
	first := order[0]
	mu.Unlock()
	assert.Equal(t, "urgent", first)

	// Drain the slow batch and verify completion order and timestamps.
	pool.StopAndWait()

	expectedOrder := []string{"urgent"}
	for i := 0; i < batchSize; i++ {
		expectedOrder = append(expectedOrder, fmt.Sprintf("low-%02d", i))
	}

	mu.Lock()
	actualOrder := append([]string(nil), order...)
	urgentAt := finishedAt["urgent"]
	mu.Unlock()

	if !reflect.DeepEqual(expectedOrder, actualOrder) {
		t.Fatalf("completion order mismatch:\nexpected %v\nactual   %v", expectedOrder, actualOrder)
	}

	for i := 0; i < batchSize; i++ {
		name := fmt.Sprintf("low-%02d", i)
		mu.Lock()
		lowAt := finishedAt[name]
		mu.Unlock()

		if !urgentAt.Before(lowAt) {
			t.Fatalf("urgent task finished at %v, but %s finished at %v", urgentAt, name, lowAt)
		}
	}
}

func TestPoolPriorityFIFOWithinSameLevel(t *testing.T) {
	gated := newGatedSingleWorkerPool(t)
	pool := gated.pool

	var mu sync.Mutex
	order := make([]int, 0, 7)

	plan := []struct {
		id       int
		priority Priority
	}{
		{0, PriorityHigh},
		{1, PriorityLow},
		{2, PriorityNormal},
		{3, PriorityHigh},
		{4, PriorityLow},
		{5, PriorityNormal},
		{6, PriorityHigh},
	}

	for _, item := range plan {
		item := item
		if err := pool.GoWithPriority(item.priority, func() {
			mu.Lock()
			order = append(order, item.id)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("submit task %d: %v", item.id, err)
		}
	}

	gated.release()
	pool.StopAndWait()

	expected := []int{0, 3, 6, 2, 5, 1, 4}

	mu.Lock()
	actual := append([]int(nil), order...)
	mu.Unlock()

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("same-level FIFO order mismatch:\nexpected %v\nactual   %v", expected, actual)
	}
}

func TestPoolPriorityWithTaskOption(t *testing.T) {
	gated := newGatedSingleWorkerPool(t)
	pool := gated.pool

	var mu sync.Mutex
	order := make([]string, 0, 2)

	if err := pool.GoWithPriority(PriorityLow, func() {
		mu.Lock()
		order = append(order, "low")
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	future, accepted := pool.TrySubmit(func() {
		mu.Lock()
		order = append(order, "high")
		mu.Unlock()
	}, WithPriority(PriorityHigh))
	assert.True(t, accepted)

	gated.release()
	pool.StopAndWait()

	if err := future.Wait(); err != nil {
		t.Fatal(err)
	}

	expected := []string{"high", "low"}

	mu.Lock()
	actual := append([]string(nil), order...)
	mu.Unlock()

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("option priority order mismatch: expected %v, got %v", expected, actual)
	}
}

func TestPoolPriorityLowCompletesAfterHighBurst(t *testing.T) {
	gated := newGatedSingleWorkerPool(t)
	pool := gated.pool

	lowDone := make(chan struct{})
	if err := pool.GoWithPriority(PriorityLow, func() {
		close(lowDone)
	}); err != nil {
		t.Fatal(err)
	}

	highTasks := defaultPriorityOvertakeLimit * 4
	for i := 0; i < highTasks; i++ {
		if err := pool.GoWithPriority(PriorityHigh, func() {}); err != nil {
			t.Fatalf("submit high task %d: %v", i, err)
		}
	}

	gated.release()

	select {
	case <-lowDone:
	case <-time.After(10 * time.Second):
		t.Fatal("low priority task starved behind a finite burst of high priority tasks")
	}
}

func TestPoolPriorityLowNotStarvedUnderContinuousHighLoad(t *testing.T) {
	gated := newGatedSingleWorkerPool(t, WithQueueSize(128), WithNonBlocking(true))
	pool := gated.pool

	lowDone := make(chan struct{})
	if err := pool.GoWithPriority(PriorityLow, func() {
		close(lowDone)
	}); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var submitters sync.WaitGroup
	submitters.Add(1)

	go func() {
		defer submitters.Done()

		for !stop.Load() {
			_ = pool.GoWithPriority(PriorityHigh, func() {})
			runtime.Gosched()
		}
	}()

	gated.release()

	select {
	case <-lowDone:
	case <-time.After(10 * time.Second):
		stop.Store(true)
		submitters.Wait()
		t.Fatal("low priority task starved under continuous high priority submissions")
	}

	stop.Store(true)
	submitters.Wait()
}

func TestPoolPriorityQueueSizeIsSharedAcrossLevels(t *testing.T) {
	gated := newGatedSingleWorkerPool(t, WithQueueSize(2))
	pool := gated.pool

	_, firstAccepted := pool.TrySubmitWithPriority(PriorityLow, func() {})
	assert.True(t, firstAccepted)

	_, secondAccepted := pool.TrySubmit(func() {}, WithPriority(PriorityHigh))
	assert.True(t, secondAccepted)

	_, thirdAccepted := pool.TrySubmitWithPriority(PriorityNormal, func() {})
	if thirdAccepted {
		t.Fatal("expected third queued task to be rejected when the shared queue is full")
	}

	assert.Equal(t, uint64(2), pool.WaitingTasks())
	assert.Equal(t, uint64(1), pool.DroppedTasks())
}

func TestResultPoolPrioritySubmission(t *testing.T) {
	pool := NewResultPool[int](1)
	defer pool.StopAndWait()

	highFuture := pool.SubmitWithPriority(PriorityHigh, func() int {
		return 42
	})
	value, err := highFuture.Wait()
	assert.Equal(t, nil, err)
	assert.Equal(t, 42, value)

	lowFuture := pool.Submit(func() int {
		return 7
	}, WithPriority(PriorityLow))
	value, err = lowFuture.Wait()
	assert.Equal(t, nil, err)
	assert.Equal(t, 7, value)

	errFuture := pool.SubmitErrWithPriority(PriorityUrgent, func() (int, error) {
		return 1, nil
	})
	value, err = errFuture.Wait()
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, value)

	tryFuture, accepted := pool.TrySubmitWithPriority(PriorityNormal, func() int {
		return 99
	})
	assert.True(t, accepted)
	value, err = tryFuture.Wait()
	assert.Equal(t, nil, err)
	assert.Equal(t, 99, value)
}

func TestDefaultPoolSubmitWithPriority(t *testing.T) {
	task := SubmitWithPriority(PriorityHigh, func() {})
	assert.Equal(t, nil, task.Wait())

	errTask := SubmitErrWithPriority(PriorityLow, func() error {
		return nil
	})
	assert.Equal(t, nil, errTask.Wait())
}
