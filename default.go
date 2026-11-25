package pond

// defaultPool is the default pool used by the package-level functions.
var defaultPool = newPool(0, nil)

// Submit submits a task to the default pool and returns a future that can be used to wait for the task to complete.
// Task options such as WithPriority may be passed to set the priority.
func Submit(task func(), options ...TaskOption) Task {
	return defaultPool.Submit(task, options...)
}

// SubmitWithPriority submits a task to the default pool at the given priority
// and returns a future that can be used to wait for the task to complete.
func SubmitWithPriority(priority Priority, task func()) Task {
	return defaultPool.SubmitWithPriority(priority, task)
}

// SubmitErr submits a task to the default pool and returns a future that can be used to wait for the task to complete.
// Task options such as WithPriority may be passed to set the priority.
func SubmitErr(task func() error, options ...TaskOption) Task {
	return defaultPool.SubmitErr(task, options...)
}

// SubmitErrWithPriority submits an error-returning task to the default pool at
// the given priority and returns a future.
func SubmitErrWithPriority(priority Priority, task func() error) Task {
	return defaultPool.SubmitErrWithPriority(priority, task)
}

// NewGroup creates a new task group with the default pool.
func NewGroup() TaskGroup {
	return defaultPool.NewGroup()
}

// NewSubpool creates a new subpool with the default pool.
func NewSubpool(maxConcurrency int) Pool {
	return defaultPool.NewSubpool(maxConcurrency)
}
