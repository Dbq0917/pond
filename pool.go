package pond

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/alitto/pond/v2/internal/future"
	"github.com/alitto/pond/v2/internal/linkedbuffer"
)

const (
	// Constant for an unbounded queue
	Unbounded               = math.MaxInt
	DefaultQueueSize        = Unbounded
	DefaultNonBlocking      = false
	LinkedBufferInitialSize = 1024
	LinkedBufferMaxCapacity = 100 * 1024
)

var (
	ErrQueueFull             = errors.New("queue is full")
	ErrQueueEmpty            = errors.New("queue is empty")
	ErrPoolStopped           = errors.New("pool stopped")
	ErrMaxConcurrencyReached = errors.New("max concurrency reached")

	poolStoppedFuture = func() Task {
		future, resolve := future.NewFuture(context.Background())
		resolve(ErrPoolStopped)
		return future
	}()
)

// BasePool defines methods common to all pool types.
type BasePool interface {
	// Returns the number of worker goroutines that are currently active (executing a task) in the pool.
	RunningWorkers() int64

	// Returns the total number of tasks accepted by the pool since its creation.
	// Rejected submissions are accounted for by DroppedTasks instead.
	SubmittedTasks() uint64

	// Returns the number of tasks that are currently waiting in the pool's queue.
	WaitingTasks() uint64

	// Returns the number of tasks that have completed with an error.
	FailedTasks() uint64

	// Returns the number of tasks that have completed successfully.
	SuccessfulTasks() uint64

	// Returns the total number of tasks that have completed (either successfully or with an error).
	// Tasks accepted by the pool but canceled before execution are excluded.
	CompletedTasks() uint64

	// Returns the number of tasks that have been dropped because the queue was full.
	DroppedTasks() uint64

	// Returns the number of tasks accepted by the pool that were canceled
	// before executing user code due to pool context cancellation.
	CanceledTasks() uint64

	// Returns the maximum concurrency of the pool.
	MaxConcurrency() int

	// Returns the size of the task queue.
	QueueSize() int

	// Returns true if the pool is non-blocking, meaning that it will not block when the task queue is full.
	// In a non-blocking pool, tasks that cannot be submitted to the queue will be dropped.
	// By default, pools are blocking, meaning that they will block when the task queue is full.
	NonBlocking() bool

	// Returns the context associated with this pool.
	Context() context.Context

	// Stops the pool and returns a future that can be used to wait for all tasks pending to complete.
	// The pool will not accept new tasks after it has been stopped.
	Stop() Task

	// Stops the pool and waits for all tasks to complete.
	StopAndWait()

	// Returns true if the pool has been stopped or its context has been cancelled.
	Stopped() bool

	// Resizes the pool by changing the maximum concurrency (number of workers) of the pool.
	// The new max concurrency must be greater than 0.
	// If the new max concurrency is less than the current number of running workers, the pool will continue to run with the new max concurrency.
	Resize(maxConcurrency int)
}

// Represents a pool of goroutines that can execute tasks concurrently.
type Pool interface {
	BasePool

	// Submits a task to the pool without waiting for it to complete.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	Go(task func()) error

	// Submits a task to the pool and returns a future that can be used to wait for the task to complete.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	Submit(task func()) Task

	// Submits a task to the pool and returns a future that can be used to wait for the task to complete.
	// The task function must return an error.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	SubmitErr(task func() error) Task

	// Attempts to submit a task to the pool and returns a future that can be used to wait for the task to complete
	// and a boolean indicating whether the task was submitted successfully.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	TrySubmit(task func()) (Task, bool)

	// Attempts to submit a task to the pool and returns a future that can be used to wait for the task to complete
	// and a boolean indicating whether the task was submitted successfully.
	// The task function must return an error.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, the returned future will resolve to ErrPoolStopped.
	TrySubmitErr(task func() error) (Task, bool)

	// Creates a new subpool with the specified maximum concurrency and options.
	NewSubpool(maxConcurrency int, options ...Option) Pool

	// Creates a new task group.
	NewGroup() TaskGroup

	// Creates a new task group with the specified context.
	NewGroupContext(ctx context.Context) TaskGroup
}

type pool struct {
	mutex           sync.Mutex
	parent          *pool
	ctx             context.Context
	cancel          context.CancelCauseFunc
	nonBlocking     bool
	panicRecovery   bool
	maxConcurrency  int
	closed          atomic.Bool
	workerCount     int64
	workerWaitGroup sync.WaitGroup
	submitWaiters   chan struct{}
	queueSize       int
	tasks           *linkedbuffer.LinkedBuffer[any]
	metrics         poolMetrics
}

type poolMetrics struct {
	submitted  uint64
	successful uint64
	failed     uint64
	dropped    uint64
	canceled   uint64
}

func (p *pool) Context() context.Context {
	return p.ctx
}

func (p *pool) Stopped() bool {
	return p.closed.Load() || p.ctx.Err() != nil
}

func (p *pool) MaxConcurrency() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.maxConcurrency
}

func (p *pool) Resize(maxConcurrency int) {
	if maxConcurrency == 0 {
		maxConcurrency = math.MaxInt
	}

	if maxConcurrency < 0 {
		panic(errors.New("maxConcurrency must be greater than or equal to 0"))
	}

	p.mutex.Lock()

	// Workers from an earlier limit can still be running after a shrink.
	newWorkers := maxConcurrency - int(p.workerCount)
	if queuedTasks := int(p.tasks.Len()); newWorkers > queuedTasks {
		newWorkers = queuedTasks
	}

	p.maxConcurrency = maxConcurrency

	if newWorkers > 0 {
		p.workerCount += int64(newWorkers)
		p.workerWaitGroup.Add(newWorkers)
	}

	p.mutex.Unlock()

	// Launch the new workers
	for i := 0; i < newWorkers; i++ {
		p.launchWorker(nil)
	}
}

func (p *pool) QueueSize() int {
	return p.queueSize
}

func (p *pool) NonBlocking() bool {
	return p.nonBlocking
}

func (p *pool) RunningWorkers() int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.workerCount
}

func (p *pool) SubmittedTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.submitted
}

func (p *pool) WaitingTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.waitingTasksLocked()
}

func (p *pool) FailedTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.failed
}

func (p *pool) SuccessfulTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.successful
}

func (p *pool) CompletedTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.successful + p.metrics.failed
}

func (p *pool) DroppedTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.dropped
}

func (p *pool) CanceledTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.metrics.canceled
}

func (p *pool) waitingTasksLocked() uint64 {
	return p.metrics.submitted -
		p.metrics.successful -
		p.metrics.failed -
		p.metrics.canceled -
		uint64(p.workerCount)
}

func (p *pool) worker(task any) {
	var readTaskErr, err error
	exitedNormally := false
	defer func() {
		if !exitedNormally {
			// In case of abnormal exit (e.g. runtime.Goexit() in the task),
			// launch a new worker to execute the next task in the queue.
			nextTask, readErr := p.completeAndReadTask(fmt.Errorf("worker exited abnormally: %w", err))
			if readErr != nil {
				return
			}

			if nextTask != nil {
				p.launchWorker(nextTask)
			}
		}
	}()
	for {
		if task != nil {
			_, err = invokeTask[any](task, p.panicRecovery)
			task, readTaskErr = p.completeAndReadTask(err)
		} else {
			task, readTaskErr = p.readTask()
		}

		if readTaskErr != nil {
			exitedNormally = true
			return
		}
	}
}

func (p *pool) subpoolWorker(task any) func() (output any, err error) {
	return func() (output any, err error) {
		exitedNormally := false
		var nextErr error
		defer func() {
			if !exitedNormally {
				nextTask, readErr := p.completeAndReadTask(fmt.Errorf("worker exited abnormally: %w", err))
				if readErr == nil {
					p.launchWorker(nextTask)
				}
			}
		}()

		if task != nil {
			output, err = invokeTask[any](task, p.panicRecovery)
			task, nextErr = p.completeAndReadTask(err)
		} else {
			task, nextErr = p.readTask()
		}

		exitedNormally = true

		// Attempt to submit the next task to the parent pool
		for nextErr == nil && task != nil {
			submitErr := p.parent.submit(p.subpoolWorker(task), p.nonBlocking)
			if submitErr == nil {
				break
			}

			// Wrap the error with the context canceled error to reflect that the task was canceled.
			if errors.Is(submitErr, ErrPoolStopped) {
				task, nextErr = p.completeAndReadTask(errors.Join(ErrContextCanceled, submitErr))
				continue
			}

			task, nextErr = p.readTask()
			if nextErr != nil {
				break
			}
		}

		return
	}
}

func (p *pool) Go(task func()) error {
	// Wrap the task so it is skipped if the pool context is canceled while it is queued
	return p.submit(wrapTask[struct{}, func(error)](task, func(error) {}, p.ctx, p.panicRecovery), p.nonBlocking)
}

func (p *pool) Submit(task func()) Task {
	future, _ := p.wrapAndSubmit(task, p.nonBlocking)
	return future
}

func (p *pool) SubmitErr(task func() error) Task {
	future, _ := p.wrapAndSubmit(task, p.nonBlocking)
	return future
}

func (p *pool) TrySubmit(task func()) (Task, bool) {
	return p.wrapAndSubmit(task, true)
}

func (p *pool) TrySubmitErr(task func() error) (Task, bool) {
	return p.wrapAndSubmit(task, true)
}

func (p *pool) wrapAndSubmit(task any, nonBlocking bool) (Task, bool) {
	if p.Stopped() {
		return poolStoppedFuture, false
	}

	future, wrappedTask, resolve := p.wrapTask(task)

	if err := p.submit(wrappedTask, nonBlocking); err != nil {
		resolve(err)
		return future, false
	}

	return future, true
}

func (p *pool) wrapTask(task any) (Task, func() error, func(error)) {
	ctx := p.Context()
	future, resolve := future.NewFuture(ctx)

	wrappedTask := wrapTask[struct{}, func(error)](task, resolve, ctx, p.panicRecovery)

	return future, wrappedTask, resolve
}

func (p *pool) submit(task any, nonBlocking bool) (err error) {
	if nonBlocking {
		err = p.trySubmit(task, true)
	} else {
		err = p.blockingTrySubmit(task)
	}

	return
}

func (p *pool) blockingTrySubmit(task any) error {
	for {
		if err := p.trySubmit(task, false); err != ErrQueueFull {
			return err
		}

		// No space left in the queue, wait until a slot is released
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-p.submitWaiters:
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
			}
		}
	}
}

func (p *pool) trySubmit(task any, accountDropped bool) error {
	p.mutex.Lock()

	// Check if the pool has been stopped while holding the lock
	// to avoid race conditions on the workers wait group if the pool is being stopped.
	if p.Stopped() {
		p.mutex.Unlock()
		return ErrPoolStopped
	}

	queueEnabled := p.queueSize > 0
	tasksLen := int(p.tasks.Len())

	// When queue is enabled, check if it is full
	if queueEnabled && tasksLen >= p.queueSize {
		if accountDropped {
			p.metrics.dropped++
		}
		p.mutex.Unlock()
		return ErrQueueFull
	}

	if p.workerCount >= int64(p.maxConcurrency) {
		// When queue is disabled, return an error immediately if max concurrency is reached
		if !queueEnabled {
			if accountDropped {
				p.metrics.dropped++
			}
			p.mutex.Unlock()
			return ErrQueueFull
		}

		// If queue is enabled, push the task at the back of the queue
		p.metrics.submitted++
		p.tasks.Write(task)
		p.mutex.Unlock()
		return nil
	}

	p.metrics.submitted++
	p.workerCount++
	p.workerWaitGroup.Add(1)

	if queueEnabled && tasksLen > 0 {
		// Push the task at the back of the queue
		p.tasks.Write(task)

		// Pop the front task
		task, _ = p.tasks.Read()
	}

	p.mutex.Unlock()

	p.launchWorker(task)

	// Notify a submit waiter there is room in the queue for a new task
	p.notifySubmitWaiter()

	return nil
}

func (p *pool) launchWorker(task any) {
	if p.parent == nil {
		// Launch a new worker to execute the task
		go p.worker(task)
	} else {
		// Submit task to the parent pool wrapped in a function that will
		// submit the next task to the parent pool when it completes (subpool worker)
		p.parent.submit(p.subpoolWorker(task), p.nonBlocking)
	}
}

func (p *pool) readTask() (task any, err error) {
	p.mutex.Lock()
	task, err = p.readTaskLocked()
	p.mutex.Unlock()

	p.notifySubmitWaiter()

	return
}

func (p *pool) completeAndReadTask(result error) (task any, err error) {
	p.mutex.Lock()
	p.recordCompletionLocked(result)
	task, err = p.readTaskLocked()
	p.mutex.Unlock()

	p.notifySubmitWaiter()

	return
}

func (p *pool) readTaskLocked() (task any, err error) {
	if p.tasks.Len() == 0 {
		// No more tasks in the queue, worker will exit
		p.workerCount--
		p.workerWaitGroup.Done()
		err = ErrQueueEmpty
		return
	}

	if p.maxConcurrency > 0 && p.workerCount > int64(p.maxConcurrency) {
		// Max concurrency reached, kill the worker
		p.workerCount--
		p.workerWaitGroup.Done()
		err = ErrMaxConcurrencyReached
		return
	}

	task, _ = p.tasks.Read()
	return
}

func (p *pool) notifySubmitWaiter() {
	// Wake up one of the waiters (if any)
	select {
	case p.submitWaiters <- struct{}{}:
	default:
		return
	}
}

func (p *pool) recordCompletionLocked(err error) {
	if err != nil {
		if errors.Is(err, ErrContextCanceled) {
			p.metrics.canceled++
		} else {
			p.metrics.failed++
		}
	} else {
		p.metrics.successful++
	}
}

func (p *pool) Stop() Task {
	return Submit(func() {
		// Stop accepting new tasks while holding the lock to avoid race conditions.
		p.mutex.Lock()
		p.closed.Store(true)
		p.mutex.Unlock()

		// Wait for all workers to finish executing all tasks (including the ones in the queue)
		p.workerWaitGroup.Wait()

		// Cancel the context with a pool stopped error to signal that the pool has been stopped
		p.cancel(ErrPoolStopped)
	})
}

func (p *pool) StopAndWait() {
	p.Stop().Wait()
}

func (p *pool) NewSubpool(maxConcurrency int, options ...Option) Pool {
	return newPool(maxConcurrency, p, options...)
}

func (p *pool) NewGroup() TaskGroup {
	return newTaskGroup(p, p.ctx)
}

func (p *pool) NewGroupContext(ctx context.Context) TaskGroup {
	return newTaskGroup(p, ctx)
}

func newPool(maxConcurrency int, parent *pool, options ...Option) *pool {

	if parent != nil {
		if maxConcurrency > parent.MaxConcurrency() {
			panic(fmt.Errorf("maxConcurrency cannot be greater than the parent pool's maxConcurrency (%d)", parent.MaxConcurrency()))
		}

		if maxConcurrency == 0 {
			maxConcurrency = parent.MaxConcurrency()
		}
	}

	if maxConcurrency == 0 {
		maxConcurrency = math.MaxInt
	}

	if maxConcurrency < 0 {
		panic(errors.New("maxConcurrency must be greater than or equal to 0"))
	}

	pool := &pool{
		ctx:            context.Background(),
		nonBlocking:    DefaultNonBlocking,
		panicRecovery:  true,
		maxConcurrency: maxConcurrency,
		queueSize:      DefaultQueueSize,
		// Buffer size of 1 to prevent deadlock when read on the submitWaiters channel happens
		// after the write on the same channel in the notifySubmitWaiter method.
		// See https://github.com/alitto/pond/issues/108
		submitWaiters: make(chan struct{}, 1),
	}

	if parent != nil {
		pool.parent = parent
		pool.ctx = parent.Context()
		pool.queueSize = parent.queueSize
		pool.nonBlocking = parent.nonBlocking
		pool.panicRecovery = parent.panicRecovery
	}

	for _, option := range options {
		option(pool)
	}

	pool.ctx, pool.cancel = context.WithCancelCause(pool.ctx)

	pool.tasks = linkedbuffer.NewLinkedBuffer[any](LinkedBufferInitialSize, LinkedBufferMaxCapacity)

	return pool
}

// NewPool creates a new pool with the given maximum concurrency and options.
// The new maximum concurrency must be greater than or equal to 0 (0 means no limit).
func NewPool(maxConcurrency int, options ...Option) Pool {
	return newPool(maxConcurrency, nil, options...)
}
