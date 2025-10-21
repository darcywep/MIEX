package utils

import (
	"log"
	"runtime"
	"sync"
	"time"
)

type ThreadPool struct {
	workers           []worker
	stop              bool
	threadDurations   []time.Duration
	threadNum         int
	taskCounts        []int
	stopFlags         []bool
	highPriorityTasks chan func()
	lowPriorityTasks  chan func()
	queueMutex        sync.Mutex
	condition         *sync.Cond
}

// worker is a struct that holds information for each worker thread
type worker struct {
	id int
	wg *sync.WaitGroup
}

// PinRoundRobin binds a goroutine to a specific CPU core
func PinRoundRobin(threadID int, rotateID int) error {
	coreID := rotateID % runtime.NumCPU() // Get core ID
	// Setting the CPU affinity is not natively supported in Go, so we skip it
	// in the Go version. However, some specific system calls like cgo can do this if needed.
	// Placeholder to indicate core affinity setting
	log.Printf("Binding thread-%d to core %d", threadID, coreID)
	return nil
}

// workerFunc is the function executed by each worker goroutine
func (pool *ThreadPool) workerFunc(workerID int) {
	for {
		var task func()
		select {
		case task = <-pool.highPriorityTasks:
			// High-priority task
			log.Println("Processing high-priority task")
		case task = <-pool.lowPriorityTasks:
			// Low-priority task
			log.Println("Processing low-priority task")
		}

		// If stop is triggered, or the worker is flagged for stop, we exit the loop
		if pool.stop || pool.stopFlags[workerID] {
			break
		}

		// Execute the task
		task()
		// Update task count and duration
		pool.taskCounts[workerID]++
	}
}

// NewThreadPool initializes a new thread pool
func NewThreadPool(threadNum int) *ThreadPool {
	highPriorityChan := make(chan func(), threadNum)
	lowPriorityChan := make(chan func(), threadNum)
	condition := sync.NewCond(&sync.Mutex{})
	pool := &ThreadPool{
		highPriorityTasks: highPriorityChan,
		lowPriorityTasks:  lowPriorityChan,
		stopFlags:         make([]bool, threadNum),
		threadDurations:   make([]time.Duration, threadNum),
		taskCounts:        make([]int, threadNum),
		condition:         condition,
		threadNum:         threadNum,
	}

	for i := 0; i < threadNum; i++ {
		// Create workers and assign them a goroutine
		worker := worker{id: i, wg: &sync.WaitGroup{}}
		pool.workers = append(pool.workers, worker)
		worker.wg.Add(1)

		go func(workerID int) {
			defer worker.wg.Done()
			// Pin the worker to a specific core (simulated in Go version)
			err := PinRoundRobin(workerID, i)
			if err != nil {
				log.Fatalf("Error binding worker-%d to core: %v", workerID, err)
			}
			// Start the worker's execution
			pool.workerFunc(workerID)
		}(i)
	}

	return pool
}

// Enqueue adds a task to the appropriate task queue
func (pool *ThreadPool) Enqueue(task func(), priority string) {
	if pool.stop {
		log.Println("ThreadPool is stopped, cannot enqueue new tasks.")
		return
	}

	if priority == "high" {
		pool.highPriorityTasks <- task
	} else {
		pool.lowPriorityTasks <- task
	}

	pool.condition.Signal() // Signal workers to start
}

// ResizePool resizes the pool of workers
func (pool *ThreadPool) ResizePool(newSize int) {
	pool.queueMutex.Lock()
	defer pool.queueMutex.Unlock()

	currentSize := len(pool.workers)
	if newSize > currentSize {
		// Add new workers
		for i := currentSize; i < newSize; i++ {
			pool.stopFlags = append(pool.stopFlags, false)
			worker := worker{id: i, wg: &sync.WaitGroup{}}
			pool.workers = append(pool.workers, worker)
			worker.wg.Add(1)
			go func(workerID int) {
				defer worker.wg.Done()
				err := PinRoundRobin(workerID, i)
				if err != nil {
					log.Fatalf("Error binding worker-%d to core: %v", workerID, err)
				}
				pool.workerFunc(workerID)
			}(i)
		}
	} else if newSize < currentSize {
		// Remove workers by stopping them
		for i := newSize; i < currentSize; i++ {
			pool.stopFlags[i] = true
		}
		pool.condition.Broadcast() // Wake all workers to check stop condition

		// Wait for workers to finish
		for i := newSize; i < currentSize; i++ {
			pool.workers[i].wg.Wait()
		}
		pool.workers = pool.workers[:newSize]
		pool.stopFlags = pool.stopFlags[:newSize]
	}

	pool.threadNum = newSize
	log.Printf("Thread pool resized to %d workers", newSize)
}

// Shutdown stops the pool and waits for all workers to finish
func (pool *ThreadPool) Shutdown() {
	pool.queueMutex.Lock()
	pool.stop = true
	pool.queueMutex.Unlock()
	pool.condition.Broadcast() // Wake all workers to check stop condition

	for _, worker := range pool.workers {
		worker.wg.Wait() // Wait for each worker to finish
	}
}

// GetThreadDurations returns the durations for each thread's work
func (pool *ThreadPool) GetThreadDurations() []time.Duration {
	return pool.threadDurations
}

// GetTaskCounts returns the number of tasks processed by each thread
func (pool *ThreadPool) GetTaskCounts() []int {
	return pool.taskCounts
}

func main() {
	// Initialize the thread pool with 4 threads
	pool := NewThreadPool(4)

	// Enqueue some tasks
	for i := 0; i < 10; i++ {
		task := func() {
			log.Printf("Processing task %d", i)
		}
		priority := "low"
		if i%2 == 0 {
			priority = "high"
		}
		pool.Enqueue(task, priority)
	}

	// Resize the pool to 6 threads
	pool.ResizePool(6)

	// Shutdown the pool after all tasks are finished
	pool.Shutdown()
}
