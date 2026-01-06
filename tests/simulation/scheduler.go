package simulation

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Scheduler provides deterministic control over concurrent operations.
// It allows controlling the order in which operations proceed, ensuring
// reproducible tests for concurrent scenarios.
type Scheduler struct {
	mu sync.Mutex

	// Task tracking
	tasks    map[uint64]*Task
	taskSeq  atomic.Uint64
	taskCond *sync.Cond

	// Execution control
	mode          SchedulerMode
	allowedCounts map[uint64]int      // How many steps allowed per task
	blockedTasks  map[uint64]bool
	pausedAt      map[uint64]string // Task ID -> pause point name

	// For sequential mode
	runQueue []uint64

	// Completion tracking
	completed map[uint64]struct{}

	clock *SimClock
}

// SchedulerMode controls how tasks are scheduled.
type SchedulerMode int

const (
	// ModeParallel allows all tasks to run freely.
	ModeParallel SchedulerMode = iota
	// ModeSequential runs one task at a time in queue order.
	ModeSequential
	// ModeStep allows manual stepping through tasks.
	ModeStep
)

// Task represents a schedulable unit of work.
type Task struct {
	ID        uint64
	Name      string
	Created   time.Time
	Started   time.Time
	Completed time.Time

	mu       sync.Mutex
	cond     *sync.Cond
	state    TaskState
	result   any
	err      error
	paused   bool
	pausedAt string

	scheduler *Scheduler
	cancelFn  context.CancelFunc
}

// TaskState represents the state of a task.
type TaskState int

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskPaused
	TaskCompleted
	TaskFailed
	TaskCancelled
)

// NewScheduler creates a new deterministic scheduler.
func NewScheduler(clock *SimClock) *Scheduler {
	if clock == nil {
		clock = NewSimClock()
	}
	s := &Scheduler{
		tasks:         make(map[uint64]*Task),
		allowedCounts: make(map[uint64]int),
		blockedTasks:  make(map[uint64]bool),
		pausedAt:      make(map[uint64]string),
		completed:     make(map[uint64]struct{}),
		clock:         clock,
	}
	s.taskCond = sync.NewCond(&s.mu)
	return s
}

// SetMode sets the scheduling mode.
func (s *Scheduler) SetMode(mode SchedulerMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
}

// Submit creates a new task that will execute the given function.
func (s *Scheduler) Submit(name string, fn func(ctx context.Context) (any, error)) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.taskSeq.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	task := &Task{
		ID:        id,
		Name:      name,
		Created:   s.clock.Now(),
		state:     TaskPending,
		scheduler: s,
		cancelFn:  cancel,
	}
	task.cond = sync.NewCond(&task.mu)
	s.tasks[id] = task

	// In sequential mode, add to run queue
	if s.mode == ModeSequential {
		s.runQueue = append(s.runQueue, id)
	}

	go func() {
		// Wait until allowed to start
		s.waitForStart(task)

		task.mu.Lock()
		if task.state == TaskCancelled {
			task.mu.Unlock()
			return
		}
		task.state = TaskRunning
		task.Started = s.clock.Now()
		task.mu.Unlock()

		result, err := fn(ctx)

		task.mu.Lock()
		if err != nil {
			task.state = TaskFailed
			task.err = err
		} else {
			task.state = TaskCompleted
			task.result = result
		}
		task.Completed = s.clock.Now()
		task.cond.Broadcast()
		task.mu.Unlock()

		s.markComplete(id)
	}()

	return task
}

func (s *Scheduler) waitForStart(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		switch s.mode {
		case ModeParallel:
			// Always allowed
			return
		case ModeSequential:
			// Only the front of the queue can run
			if len(s.runQueue) > 0 && s.runQueue[0] == task.ID {
				return
			}
		case ModeStep:
			// Check if this task has steps remaining
			if s.allowedCounts[task.ID] > 0 {
				s.allowedCounts[task.ID]--
				return
			}
		}

		// Block if explicitly blocked
		if s.blockedTasks[task.ID] {
			s.taskCond.Wait()
			continue
		}

		s.taskCond.Wait()
	}
}

func (s *Scheduler) markComplete(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.completed[id] = struct{}{}

	// In sequential mode, pop from queue and allow next
	if s.mode == ModeSequential && len(s.runQueue) > 0 && s.runQueue[0] == id {
		s.runQueue = s.runQueue[1:]
		s.taskCond.Broadcast()
	}
}

// Step allows a task to proceed by one step.
func (s *Scheduler) Step(taskID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowedCounts[taskID]++
	s.taskCond.Broadcast()
}

// StepAll allows all pending tasks to proceed by one step.
func (s *Scheduler) StepAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.tasks {
		if _, done := s.completed[id]; !done {
			s.allowedCounts[id]++
		}
	}
	s.taskCond.Broadcast()
}

// Block prevents a task from proceeding.
func (s *Scheduler) Block(taskID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockedTasks[taskID] = true
}

// Unblock allows a blocked task to proceed.
func (s *Scheduler) Unblock(taskID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blockedTasks, taskID)
	s.taskCond.Broadcast()
}

// UnblockAll removes all blocks.
func (s *Scheduler) UnblockAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockedTasks = make(map[uint64]bool)
	s.taskCond.Broadcast()
}

// Cancel cancels a task.
func (s *Scheduler) Cancel(taskID uint64) {
	s.mu.Lock()
	task, ok := s.tasks[taskID]
	s.mu.Unlock()

	if !ok {
		return
	}

	task.mu.Lock()
	if task.state == TaskPending {
		task.state = TaskCancelled
	}
	if task.cancelFn != nil {
		task.cancelFn()
	}
	task.cond.Broadcast()
	task.mu.Unlock()
}

// Wait waits for a task to complete with optional timeout.
func (s *Scheduler) Wait(taskID uint64, timeout time.Duration) (*Task, error) {
	s.mu.Lock()
	task, ok := s.tasks[taskID]
	s.mu.Unlock()

	if !ok {
		return nil, context.DeadlineExceeded
	}

	return task, task.Wait(timeout)
}

// WaitAll waits for all tasks to complete.
func (s *Scheduler) WaitAll(timeout time.Duration) error {
	s.mu.Lock()
	taskIDs := make([]uint64, 0, len(s.tasks))
	for id := range s.tasks {
		taskIDs = append(taskIDs, id)
	}
	s.mu.Unlock()

	for _, id := range taskIDs {
		if _, err := s.Wait(id, timeout); err != nil {
			return err
		}
	}
	return nil
}

// GetTask returns a task by ID.
func (s *Scheduler) GetTask(id uint64) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tasks[id]
}

// GetTasks returns all tasks.
func (s *Scheduler) GetTasks() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	return tasks
}

// RunInOrder runs tasks in the specified order.
func (s *Scheduler) RunInOrder(taskIDs []uint64) {
	s.mu.Lock()
	s.mode = ModeStep
	s.mu.Unlock()

	for _, id := range taskIDs {
		s.Step(id)
		s.Wait(id, 10*time.Second)
	}
}

// Reset clears all tasks and state.
func (s *Scheduler) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, task := range s.tasks {
		if task.cancelFn != nil {
			task.cancelFn()
		}
	}

	s.tasks = make(map[uint64]*Task)
	s.allowedCounts = make(map[uint64]int)
	s.blockedTasks = make(map[uint64]bool)
	s.pausedAt = make(map[uint64]string)
	s.completed = make(map[uint64]struct{})
	s.runQueue = nil
}

// Wait waits for the task to complete.
func (t *Task) Wait(timeout time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	deadline := time.Now().Add(timeout)
	for t.state == TaskPending || t.state == TaskRunning || t.state == TaskPaused {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		t.cond.Wait()
	}
	return t.err
}

// Result returns the task result.
func (t *Task) Result() any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result
}

// Error returns the task error.
func (t *Task) Error() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// State returns the current task state.
func (t *Task) State() TaskState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// Checkpoint creates a pause point in the task.
// Call this from within a task to allow the scheduler to pause execution.
func (t *Task) Checkpoint(name string) {
	t.mu.Lock()
	if t.paused {
		t.pausedAt = name
		t.state = TaskPaused
		t.cond.Broadcast()
		for t.paused {
			t.cond.Wait()
		}
		t.state = TaskRunning
	}
	t.mu.Unlock()
}

// Pause sets a task to pause at the next checkpoint.
func (t *Task) Pause() {
	t.mu.Lock()
	t.paused = true
	t.mu.Unlock()
}

// Resume allows a paused task to continue.
func (t *Task) Resume() {
	t.mu.Lock()
	t.paused = false
	t.pausedAt = ""
	t.cond.Broadcast()
	t.mu.Unlock()
}

// PausedAt returns the name of the checkpoint where the task is paused.
func (t *Task) PausedAt() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pausedAt
}

// WorkerPool provides a deterministic worker pool for simulation.
type WorkerPool struct {
	scheduler *Scheduler
	workers   int
	workChan  chan func()
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewWorkerPool creates a new deterministic worker pool.
func NewWorkerPool(scheduler *Scheduler, workers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &WorkerPool{
		scheduler: scheduler,
		workers:   workers,
		workChan:  make(chan func(), workers*10),
		ctx:       ctx,
		cancel:    cancel,
	}

	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}

	return pool
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case fn, ok := <-p.workChan:
			if !ok {
				return
			}
			fn()
		}
	}
}

// Submit adds work to the pool.
func (p *WorkerPool) Submit(fn func()) {
	select {
	case <-p.ctx.Done():
		return
	case p.workChan <- fn:
	}
}

// Close shuts down the worker pool.
func (p *WorkerPool) Close() {
	p.cancel()
	close(p.workChan)
	p.wg.Wait()
}
