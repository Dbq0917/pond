package pond

import (
	"context"

	"github.com/alitto/pond/v2/internal/future"
)

// ResultPool is a pool that can be used to submit tasks that return a result.
type ResultPool[R any] interface {
	BasePool

	// Submits a task to the pool and returns a future that can be used to wait for the task to complete and get the result.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	Submit(task func() R, options ...TaskOption) ResultTask[R]

	// Submits a task to the pool and returns a future that can be used to wait for the task to complete and get the result.
	// The task function must return a result and an error.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	SubmitErr(task func() (R, error), options ...TaskOption) ResultTask[R]

	// Attempts to submit a task to the pool and returns a future that can be used to wait for the task to complete
	// and a boolean indicating whether the task was submitted successfully.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	TrySubmit(task func() R, options ...TaskOption) (ResultTask[R], bool)

	// Attempts to submit a task to the pool and returns a future that can be used to wait for the task to complete
	// and a boolean indicating whether the task was submitted successfully.
	// The task function must return a result and an error.
	// The pool will not accept new tasks after it has been stopped.
	// If the pool has been stopped, this method will return ErrPoolStopped.
	TrySubmitErr(task func() (R, error), options ...TaskOption) (ResultTask[R], bool)

	// SubmitWithPriority submits a task at the given priority and returns a future.
	// Higher priority tasks jump ahead of lower priority queued tasks.
	SubmitWithPriority(priority Priority, task func() R) ResultTask[R]

	// SubmitErrWithPriority submits an error-returning task at the given priority.
	SubmitErrWithPriority(priority Priority, task func() (R, error)) ResultTask[R]

	// TrySubmitWithPriority attempts a non-blocking submission at the given priority.
	TrySubmitWithPriority(priority Priority, task func() R) (ResultTask[R], bool)

	// TrySubmitErrWithPriority is the error-returning variant of
	// TrySubmitWithPriority.
	TrySubmitErrWithPriority(priority Priority, task func() (R, error)) (ResultTask[R], bool)

	// Creates a new subpool with the specified maximum concurrency and options.
	NewSubpool(maxConcurrency int, options ...Option) ResultPool[R]

	// Creates a new task group.
	NewGroup() ResultTaskGroup[R]

	// Creates a new task group with the specified context.
	NewGroupContext(ctx context.Context) ResultTaskGroup[R]
}

type resultPool[R any] struct {
	*pool
}

func (p *resultPool[R]) NewGroup() ResultTaskGroup[R] {
	return newResultTaskGroup[R](p.pool, p.Context())
}

func (p *resultPool[R]) NewGroupContext(ctx context.Context) ResultTaskGroup[R] {
	return newResultTaskGroup[R](p.pool, ctx)
}

func (p *resultPool[R]) Submit(task func() R, options ...TaskOption) ResultTask[R] {
	config := resolveTaskConfig(options)
	future, _ := p.submitWithPriority(task, config.priority, p.nonBlocking)
	return future
}

func (p *resultPool[R]) SubmitErr(task func() (R, error), options ...TaskOption) ResultTask[R] {
	config := resolveTaskConfig(options)
	future, _ := p.submitWithPriority(task, config.priority, p.nonBlocking)
	return future
}

func (p *resultPool[R]) TrySubmit(task func() R, options ...TaskOption) (ResultTask[R], bool) {
	config := resolveTaskConfig(options)
	return p.submitWithPriority(task, config.priority, true)
}

func (p *resultPool[R]) TrySubmitErr(task func() (R, error), options ...TaskOption) (ResultTask[R], bool) {
	config := resolveTaskConfig(options)
	return p.submitWithPriority(task, config.priority, true)
}

func (p *resultPool[R]) SubmitWithPriority(priority Priority, task func() R) ResultTask[R] {
	return p.Submit(task, WithPriority(priority))
}

func (p *resultPool[R]) SubmitErrWithPriority(priority Priority, task func() (R, error)) ResultTask[R] {
	return p.SubmitErr(task, WithPriority(priority))
}

func (p *resultPool[R]) TrySubmitWithPriority(priority Priority, task func() R) (ResultTask[R], bool) {
	return p.TrySubmit(task, WithPriority(priority))
}

func (p *resultPool[R]) TrySubmitErrWithPriority(priority Priority, task func() (R, error)) (ResultTask[R], bool) {
	return p.TrySubmitErr(task, WithPriority(priority))
}

func (p *resultPool[R]) submitWithPriority(task any, priority Priority, nonBlocking bool) (ResultTask[R], bool) {
	ctx := p.Context()
	future, resolve := future.NewValueFuture[R](ctx)

	if p.Stopped() {
		var zero R
		resolve(zero, ErrPoolStopped)
		return future, false
	}

	wrapped := wrapTask[R, func(R, error)](task, resolve, ctx, p.pool.panicRecovery)

	if err := p.pool.submitWithPriority(wrapped, priority, nonBlocking); err != nil {
		var zero R
		resolve(zero, err)
		return future, false
	}

	return future, true
}

func (p *resultPool[R]) NewSubpool(maxConcurrency int, options ...Option) ResultPool[R] {
	return newResultPool[R](maxConcurrency, p.pool, options...)
}

func newResultPool[R any](maxConcurrency int, parent *pool, options ...Option) *resultPool[R] {
	return &resultPool[R]{
		pool: newPool(maxConcurrency, parent, options...),
	}
}

// NewResultPool creates a new result pool with the given maximum concurrency and options.
// Result pools are generic pools that can be used to submit tasks that return a result.
// The new maximum concurrency must be greater than or equal to 0 (0 means no limit).
func NewResultPool[R any](maxConcurrency int, options ...Option) ResultPool[R] {
	return newResultPool[R](maxConcurrency, nil, options...)
}
