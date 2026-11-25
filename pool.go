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

	// Returns the total number of tasks submitted to the pool since its creation.
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
	mutex               sync.Mutex
	parent              *pool
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	nonBlocking         bool
	panicRecovery       bool
	maxConcurrency      int
	closed              atomic.Bool
	workerCount         atomic.Int64
	workerWaitGroup     sync.WaitGroup
	submitWaiters       chan struct{}
	queueSize           int
	tasks               *linkedbuffer.LinkedBuffer[*poolTask]
	submittedTaskCount  atomic.Uint64
	successfulTaskCount atomic.Uint64
	failedTaskCount     atomic.Uint64
	droppedTaskCount    atomic.Uint64
	canceledTaskCount   atomic.Uint64
}

// poolTask is the unit stored in a pool's queue. The onReject callback is invoked
// if the task is accepted but can never be executed (e.g. a subpool task that the
// parent pool refuses to run). It is nil for tasks that do not need to be notified.
type poolTask struct {
	task        any
	nonBlocking bool
	onReject    func(error)
}

func (t *poolTask) invoke(panicRecovery bool) (output any, err error) {
	if t == nil || t.task == nil {
		return nil, nil
	}

	return invokeTask[any](t.task, panicRecovery)
}

func (t *poolTask) reject(err error) {
	if t != nil && t.onReject != nil {
		t.onReject(err)
	}
}

func (p *pool) Context() context.Context {
	return p.ctx
}

func (p *pool) Stopped() bool {
	if p.closed.Load() || p.ctx.Err() != nil {
		return true
	}

	// A subpool cannot accept tasks once its parent has stopped accepting them.
	if p.parent != nil {
		return p.parent.Stopped()
	}

	return false
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
	newWorkers := maxConcurrency - int(p.workerCount.Load())
	if queuedTasks := int(p.tasks.Len()); newWorkers > queuedTasks {
		newWorkers = queuedTasks
	}

	p.maxConcurrency = maxConcurrency

	if newWorkers > 0 {
		p.workerCount.Add(int64(newWorkers))
		p.workerWaitGroup.Add(newWorkers)
	}

	p.mutex.Unlock()

	// Launch the new workers
	for i := 0; i < newWorkers; i++ {
		if err := p.launchWorker(nil); err != nil {
			p.releaseWorkerSlot()
			p.recordRejectedTask(err, true)
			p.drainQueuedTasks(err)
		}
	}
}

func (p *pool) QueueSize() int {
	return p.queueSize
}

func (p *pool) NonBlocking() bool {
	return p.nonBlocking
}

func (p *pool) RunningWorkers() int64 {
	return p.workerCount.Load()
}

func (p *pool) SubmittedTasks() uint64 {
	return p.submittedTaskCount.Load()
}

func (p *pool) WaitingTasks() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.tasks.Len()
}

func (p *pool) FailedTasks() uint64 {
	return p.failedTaskCount.Load()
}

func (p *pool) SuccessfulTasks() uint64 {
	return p.successfulTaskCount.Load()
}

func (p *pool) CompletedTasks() uint64 {
	return p.successfulTaskCount.Load() + p.failedTaskCount.Load()
}

func (p *pool) DroppedTasks() uint64 {
	return p.droppedTaskCount.Load()
}

func (p *pool) CanceledTasks() uint64 {
	return p.canceledTaskCount.Load()
}

func (p *pool) worker(task *poolTask) {
	var readTaskErr, err error
	exitedNormally := false
	defer func() {
		if !exitedNormally {
			// In case of abnormal exit (e.g. runtime.Goexit() in the task),
			// launch a new worker to execute the next task in the queue.
			p.updateMetrics(fmt.Errorf("worker exited abnormally: %w", err))

			task, err := p.readTask()
			if err != nil {
				return
			}

			if task != nil {
				p.launchWorker(task)
				p.notifySubmitWaiter()
			}
		}
	}()
	for {
		if task != nil {
			_, err = task.invoke(p.panicRecovery)

			p.updateMetrics(err)
		}

		task, readTaskErr = p.readTask()

		if readTaskErr != nil {
			exitedNormally = true
			return
		}
	}
}

func (p *pool) subpoolWorker(entry *poolTask) func() (output any, err error) {
	return func() (output any, err error) {
		exitedNormally := false
		defer func() {
			if !exitedNormally {
				p.updateMetrics(fmt.Errorf("worker exited abnormally: %w", err))
				if nextTask, readErr := p.readTask(); readErr == nil {
					if launchErr := p.launchWorker(nextTask); launchErr != nil {
						p.handleForwardFailure(nextTask, launchErr)
					}
				}
			}
		}()

		if entry != nil && entry.task != nil {
			output, err = entry.invoke(p.panicRecovery)

			p.updateMetrics(err)
		}

		exitedNormally = true

		// Attempt to submit the next task to the parent pool
		if nextTask, readErr := p.readTask(); readErr == nil {
			if launchErr := p.launchWorker(nextTask); launchErr != nil {
				p.handleForwardFailure(nextTask, launchErr)
			}
		}

		return
	}
}

func (p *pool) Go(task func()) error {
	// Wrap the task so it is skipped if the pool context is canceled while it is queued
	return p.submit(wrapTask[struct{}, func(error)](task, func(error) {}, p.ctx, p.panicRecovery), p.nonBlocking, nil)
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

	if err := p.submit(wrappedTask, nonBlocking, resolve); err != nil {
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

func (p *pool) submit(task any, nonBlocking bool, onReject func(error)) (err error) {

	p.submittedTaskCount.Add(1)
	entry := &poolTask{
		task:        task,
		nonBlocking: nonBlocking,
		onReject:    onReject,
	}

	if nonBlocking {
		err = p.trySubmit(entry)
	} else {
		err = p.blockingTrySubmit(entry)
	}

	if err != nil {
		p.droppedTaskCount.Add(1)
	}

	return
}

func (p *pool) blockingTrySubmit(entry *poolTask) error {
	for {
		if err := p.trySubmit(entry); err != ErrQueueFull {
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

func (p *pool) trySubmit(entry *poolTask) error {
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
		p.mutex.Unlock()
		return ErrQueueFull
	}

	if int(p.workerCount.Load()) >= p.maxConcurrency {
		// When queue is disabled, return an error immediately if max concurrency is reached
		if !queueEnabled {
			p.mutex.Unlock()
			return ErrQueueFull
		}

		// If queue is enabled, push the task at the back of the queue
		p.tasks.Write(entry)
		p.mutex.Unlock()
		return nil
	}

	p.workerCount.Add(1)
	p.workerWaitGroup.Add(1)

	launchedTask := entry
	if queueEnabled && tasksLen > 0 {
		// Push the task at the back of the queue
		p.tasks.Write(entry)

		// Pop the front task
		launchedTask, _ = p.tasks.Read()
	}

	p.mutex.Unlock()

	if err := p.launchWorker(launchedTask); err != nil {
		p.releaseWorkerSlot()

		if launchedTask == entry {
			return err
		}

		// The submitted entry is still queued, but the worker that was going to
		// process an older task could not be started. Fail the older task and
		// drain the queue so accepted tasks cannot get stranded without a worker.
		p.recordRejectedTask(err, true)
		p.drainQueuedTasks(err)
		launchedTask.reject(err)
		return nil
	}

	// Notify a submit waiter there is room in the queue for a new task
	p.notifySubmitWaiter()

	return nil
}

func (p *pool) launchWorker(entry *poolTask) error {
	if p.parent == nil {
		// Launch a new worker to execute the task
		go p.worker(entry)
		return nil
	}

	// Submit task to the parent pool wrapped in a function that will
	// submit the next task to the parent pool when it completes (subpool worker)
	nonBlocking := p.nonBlocking
	if entry != nil {
		nonBlocking = entry.nonBlocking
	}
	return p.parent.submit(p.subpoolWorker(entry), nonBlocking, p.forwardReject(entry))
}

func (p *pool) readTask() (task *poolTask, err error) {
	p.mutex.Lock()

	if p.tasks.Len() == 0 {
		// No more tasks in the queue, worker will exit
		p.workerCount.Add(-1)
		p.workerWaitGroup.Done()
		p.mutex.Unlock()

		// Notify a submit waiter there is room in the queue for a new task
		p.notifySubmitWaiter()

		err = ErrQueueEmpty
		return
	}

	if p.maxConcurrency > 0 && int(p.workerCount.Load()) > p.maxConcurrency {
		// Max concurrency reached, kill the worker
		p.workerCount.Add(-1)
		p.workerWaitGroup.Done()
		p.mutex.Unlock()

		err = ErrMaxConcurrencyReached
		return
	}

	task, _ = p.tasks.Read()

	p.mutex.Unlock()

	// Notify a submit waiter there is room in the queue for a new task
	p.notifySubmitWaiter()

	return
}

func (p *pool) releaseWorkerSlot() {
	p.workerCount.Add(-1)
	p.workerWaitGroup.Done()

	// Notify a submit waiter there is room for a new task
	p.notifySubmitWaiter()
}

func (p *pool) forwardReject(entry *poolTask) func(error) {
	return func(err error) {
		// The task was accepted by the parent pool but will never be executed,
		// so release the subpool worker that was reserved for it.
		p.releaseWorkerSlot()
		p.recordRejectedTask(err, false)
		p.drainQueuedTasks(err)
		entry.reject(err)
	}
}

func (p *pool) handleForwardFailure(entry *poolTask, err error) {
	// This is the error returned by the parent pool while forwarding a task.
	// The current subpool worker will not be able to continue the chain, so
	// release its slot and notify the caller about the failed task.
	p.releaseWorkerSlot()
	p.recordRejectedTask(err, true)
	p.drainQueuedTasks(err)
	entry.reject(err)
}

func (p *pool) recordRejectedTask(err error, parentAttempted bool) {
	switch {
	case errors.Is(err, ErrQueueFull):
		// The parent pool is full. The caller can retry the submission.
		p.droppedTaskCount.Add(1)
	case errors.Is(err, ErrPoolStopped),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		// The parent pool is stopping or its context has been canceled.
		joinedErr := errors.Join(ErrContextCanceled, err)
		p.updateMetrics(joinedErr)
		if parentAttempted && p.parent != nil {
			p.parent.updateMetrics(joinedErr)
		}
	default:
		// An unexpected rejection (e.g. an invalid submission) is a failure.
		p.updateMetrics(err)
		if parentAttempted && p.parent != nil {
			p.parent.updateMetrics(err)
		}
	}
}

func (p *pool) drainQueuedTasks(err error) {
	defer p.notifySubmitWaiter()

	for {
		entry, ok := p.popQueuedTask()
		if !ok {
			return
		}

		p.recordRejectedTask(err, false)
		entry.reject(err)
	}
}

func (p *pool) popQueuedTask() (*poolTask, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.tasks.Len() == 0 {
		return nil, false
	}

	entry, _ := p.tasks.Read()
	return entry, true
}

func (p *pool) notifySubmitWaiter() {
	// Wake up one of the waiters (if any)
	select {
	case p.submitWaiters <- struct{}{}:
	default:
		return
	}
}

func (p *pool) updateMetrics(err error) {
	if err != nil {
		if errors.Is(err, ErrContextCanceled) {
			p.canceledTaskCount.Add(1)
		} else {
			p.failedTaskCount.Add(1)
		}
	} else {
		p.successfulTaskCount.Add(1)
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

	pool.tasks = linkedbuffer.NewLinkedBuffer[*poolTask](LinkedBufferInitialSize, LinkedBufferMaxCapacity)

	return pool
}

// NewPool creates a new pool with the given maximum concurrency and options.
// The new maximum concurrency must be greater than or equal to 0 (0 means no limit).
func NewPool(maxConcurrency int, options ...Option) Pool {
	return newPool(maxConcurrency, nil, options...)
}
