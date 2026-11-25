package pond

import (
	"github.com/alitto/pond/v2/internal/linkedbuffer"
)

// Priority is the scheduling priority of a task.
//
// Higher values are served first. The zero value (PriorityNormal) is the
// priority used by tasks submitted through the regular methods, so existing
// code keeps FIFO semantics when priorities are not used.
type Priority int

const (
	// PriorityLow is intended for background, deferrable work such as batch
	// uploads. Tasks at this level may be overtaken by higher priority tasks,
	// but are never allowed to starve (see priorityQueue).
	PriorityLow Priority = iota - 1

	// PriorityNormal is the default priority. It is also the zero value.
	PriorityNormal

	// PriorityHigh is intended for urgent, latency sensitive tasks.
	PriorityHigh
)

// Convenience aliases matching common naming for the three priority tiers.
const (
	// PriorityBackground is an alias for PriorityLow.
	PriorityBackground = PriorityLow
	// PriorityDefault is an alias for PriorityNormal.
	PriorityDefault = PriorityNormal
	// PriorityUrgent is an alias for PriorityHigh.
	PriorityUrgent = PriorityHigh
)

// TaskOption configures a single task submission.
type TaskOption func(*taskConfig)

type taskConfig struct {
	priority Priority
}

// WithPriority returns a TaskOption that submits the task at the given
// priority. It can be passed to Submit, SubmitErr, Go, TrySubmit and their
// result-pool counterparts:
//
//	pool.Submit(urgentTask, pond.WithPriority(pond.PriorityHigh))
func WithPriority(priority Priority) TaskOption {
	return func(config *taskConfig) {
		config.priority = priority
	}
}

func resolveTaskConfig(options []TaskOption) taskConfig {
	config := taskConfig{priority: PriorityNormal}

	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	return config
}

const (
	priorityLowLevel    = 0
	priorityNormalLevel = 1
	priorityHighLevel   = 2
	priorityLevelCount  = 3

	// defaultPriorityOvertakeLimit bounds how many higher priority tasks may
	// overtake a waiting lower priority task before the lower priority lane is
	// served. This is what prevents low priority tasks from starving while a
	// stream of high priority tasks keeps arriving.
	defaultPriorityOvertakeLimit = 32
)

// priorityLevel maps a Priority to its fixed FIFO lane. Priorities outside the
// named range are clamped to the nearest lane so the number of lanes (and
// therefore memory) stays bounded regardless of the numeric values callers use.
func priorityLevel(priority Priority) int {
	switch {
	case priority < PriorityNormal:
		return priorityLowLevel
	case priority > PriorityNormal:
		return priorityHighLevel
	default:
		return priorityNormalLevel
	}
}

// priorityQueue holds one FIFO queue per priority level.
//
// Selection is strict-priority (the highest non-empty lane is popped first),
// with a bounded-overtake fairness rule: once a non-empty lane has been
// overtaken defaultPriorityOvertakeLimit times without being selected, the most
// overtaken lane is served next. This guarantees that every non-empty lane makes
// progress within a bounded number of pops even under an endless stream of
// higher priority tasks.
//
// priorityQueue is not safe for concurrent use. All operations run inside the
// existing pool.mutex critical section, so adding priorities introduces no
// additional lock on the submission path.
type priorityQueue struct {
	lanes         [priorityLevelCount]*linkedbuffer.LinkedBuffer[any]
	overtakes     [priorityLevelCount]int
	size          int
	overtakeLimit int
}

func newPriorityQueue(overtakeLimit int) *priorityQueue {
	if overtakeLimit <= 0 {
		overtakeLimit = defaultPriorityOvertakeLimit
	}

	return &priorityQueue{
		overtakeLimit: overtakeLimit,
	}
}

// Len returns the total number of queued tasks across all priority lanes.
func (q *priorityQueue) Len() int {
	return q.size
}

// Push appends a task to the back of its priority lane (FIFO within a level).
func (q *priorityQueue) Push(task any, priority Priority) {
	level := priorityLevel(priority)

	if q.lanes[level] == nil {
		q.lanes[level] = linkedbuffer.NewLinkedBuffer[any](LinkedBufferInitialSize, LinkedBufferMaxCapacity)
	}

	q.lanes[level].Write(task)
	q.size++
}

// Pop removes and returns the next task according to priority and fairness.
// The boolean reports whether a task was available.
func (q *priorityQueue) Pop() (any, bool) {
	if q.size == 0 {
		return nil, false
	}

	level := q.selectLevel()

	// size is maintained under the same pool mutex and selectLevel only picks
	// a non-empty lane, so Read succeeds.
	task, _ := q.lanes[level].Read()
	q.size--
	q.accountOvertakes(level)

	return task, true
}

// selectLevel returns the lane to pop from. Caller must hold the pool mutex.
func (q *priorityQueue) selectLevel() int {
	// Fairness first: if a lane has accumulated too many overtakes, serve the
	// most overtaken non-empty lane. On a tie the lower lane (the more
	// vulnerable one) wins; any other tied lane remains due and is selected
	// on a subsequent pop.
	fairLevel := -1
	maxOvertakes := 0

	for level := 0; level < priorityLevelCount; level++ {
		if q.laneLen(level) == 0 {
			continue
		}

		if overtakes := q.overtakes[level]; overtakes >= q.overtakeLimit && overtakes > maxOvertakes {
			maxOvertakes = overtakes
			fairLevel = level
		}
	}

	if fairLevel != -1 {
		return fairLevel
	}

	// Strict priority otherwise: highest non-empty lane wins.
	for level := priorityLevelCount - 1; level >= 0; level-- {
		if q.laneLen(level) > 0 {
			return level
		}
	}

	return priorityNormalLevel
}

// accountOvertakes records that level was selected: its counter resets while
// every other still non-empty lane was overtaken by this selection. Empty lanes
// have no waiting task, so their counter is reset as well.
func (q *priorityQueue) accountOvertakes(level int) {
	for other := 0; other < priorityLevelCount; other++ {
		if q.lanes[other] == nil {
			continue
		}

		switch {
		case other == level || q.laneLen(other) == 0:
			q.overtakes[other] = 0
		default:
			q.overtakes[other]++
		}
	}
}

func (q *priorityQueue) laneLen(level int) uint64 {
	if q.lanes[level] == nil {
		return 0
	}

	return q.lanes[level].Len()
}

// GoWithPriority submits a task at the given priority without waiting for it to
// complete. Higher priority tasks are dequeued before lower priority ones.
// This is shorthand for Go(task, WithPriority(priority)).
func (p *pool) GoWithPriority(priority Priority, task func()) error {
	return p.Go(task, WithPriority(priority))
}

// SubmitWithPriority submits a task at the given priority and returns a future.
// Higher priority tasks are dequeued before lower priority ones. This is
// shorthand for Submit(task, WithPriority(priority)).
func (p *pool) SubmitWithPriority(priority Priority, task func()) Task {
	return p.Submit(task, WithPriority(priority))
}

// SubmitErrWithPriority submits an error-returning task at the given priority.
// This is shorthand for SubmitErr(task, WithPriority(priority)).
func (p *pool) SubmitErrWithPriority(priority Priority, task func() error) Task {
	return p.SubmitErr(task, WithPriority(priority))
}

// TrySubmitWithPriority attempts a non-blocking submission at the given priority.
// This is shorthand for TrySubmit(task, WithPriority(priority)).
func (p *pool) TrySubmitWithPriority(priority Priority, task func()) (Task, bool) {
	return p.TrySubmit(task, WithPriority(priority))
}

// TrySubmitErrWithPriority is the error-returning variant of
// TrySubmitWithPriority. It is shorthand for
// TrySubmitErr(task, WithPriority(priority)).
func (p *pool) TrySubmitErrWithPriority(priority Priority, task func() error) (Task, bool) {
	return p.TrySubmitErr(task, WithPriority(priority))
}
