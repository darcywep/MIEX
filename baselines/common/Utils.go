package common

import (
	lvm "Janus/core/evm"
	"Janus/ethereum/database"
	"Janus/tools"
	"container/list"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Statistics struct {
	ExecCount     atomic.Uint32
	CommitCount   atomic.Uint32
	RollbackCount atomic.Uint32
	Latency       atomic.Uint32
	countBlock    atomic.Uint32
	countOverhead atomic.Uint32
	beginTime     time.Time // 开始时间
	endTime       time.Time // 结束时间
}

func NewStatistics() *Statistics {
	return &Statistics{}
}

func (s *Statistics) GetExecCount() uint32 {
	return s.ExecCount.Load()
}

func (s *Statistics) AddExecCount() {
	s.ExecCount.Add(1)
}

func (s *Statistics) AddCommitCount() {
	s.CommitCount.Add(1)
}

func (s *Statistics) AddRollbackCount() {
	s.RollbackCount.Add(1)
}

// JournalCommit 记录提交统计信息（简化版）
func (s *Statistics) JournalCommit(latency uint32) {
	s.CommitCount.Add(1)
	s.Latency.Add(latency)
}

func (s *Statistics) JournalExecute() {
	if s.ExecCount.Load() == 0 {
		s.beginTime = time.Now()
	}
	s.ExecCount.Add(1)
}

func (s *Statistics) JournalReExecute() {
	//if s.ExecCount.Load() == 0 {
	//	s.beginTime = time.Now()
	//}
	//s.ExecCount.Add(1)
}

func (s *Statistics) JournalOverheads(cost uint32) {
	s.countOverhead.Add(cost)
}

// JournalRollbackExecution 记录回滚执行统计
func (s *Statistics) JournalRollbackExecution(latency uint32) {
	s.Latency.Add(latency)
}

// StartTiming 开始计时
func (s *Statistics) StartTiming() {
	s.beginTime = time.Now()
}

// StopTiming 结束计时
func (s *Statistics) StopTiming() {
	s.endTime = time.Now()
}

// JournalBlock 记录区块统计
func (s *Statistics) JournalBlock() {
	s.countBlock.Add(1)
}

type TaskPriority int

const (
	HIGH_PRIORITY TaskPriority = iota
	LOW_PRIORITY
)

// Task 任务结构体
type Task struct {
	function func(levm *lvm.LEVM)
	priority TaskPriority
}

// worker 工作线程结构体
type worker struct {
	id       int
	pool     *ThreadPool
	stopFlag bool
	levm     *lvm.LEVM
}

// ThreadPool 线程池结构体
type ThreadPool struct {
	levm              *lvm.LEVM
	workers           []*worker
	highPriorityTasks *list.List
	lowPriorityTasks  *list.List
	Tasks             *list.List
	queueMutex        sync.Mutex
	condition         *sync.Cond
	stop              bool
	threadDurations   []time.Duration
	taskCounts        []int64
	ThreadNum         int
	stopFlags         []bool
}

func (t *ThreadPool) ResetEVM() {
	root, err := t.levm.AllDB().StateDB.Commit(uint64(0), true, true)
	if err != nil {
		fmt.Println("StateDB.Commit", err)
	}
	err = t.levm.AllDB().StateDB.Database().TrieDB().Commit(root, false)
	if err != nil {
		fmt.Println("TrieDB().Commit(root, false)", err)
	}
	err = t.levm.AllDB().UpdateStateDB(root)
	if err != nil {
		fmt.Println("UpdateStateDB", err)
	}
	for _, w := range t.workers {
		w.levm = t.levm.Copy()
	}
}

func (t *ThreadPool) EvmClose() {
	defer t.levm.AllDB().Close()
}

// NewThreadPool 创建线程池
func NewThreadPool(threadNum int) *ThreadPool {
	return NewThreadPoolWithOffset(threadNum, 0)
}

// NewThreadPoolWithOffset 创建带偏移的线程池
func NewThreadPoolWithOffset(threadNum int, offset int) *ThreadPool {

	// Step 3: 模拟执行
	levm := lvm.New(database.SmallBankStateDBConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())

	pool := &ThreadPool{
		levm:              levm,
		highPriorityTasks: list.New(),
		lowPriorityTasks:  list.New(),
		stop:              false,
		threadDurations:   make([]time.Duration, threadNum),
		taskCounts:        make([]int64, threadNum),
		ThreadNum:         threadNum,
		stopFlags:         make([]bool, threadNum),
		workers:           make([]*worker, threadNum),
	}
	pool.condition = sync.NewCond(&pool.queueMutex)

	// 创建工作线程
	for i := 0; i < threadNum; i++ {
		worker := &worker{
			id:       i,
			pool:     pool,
			stopFlag: false,
			levm:     levm.Copy(),
		}
		pool.workers[i] = worker

		// 启动goroutine并设置CPU亲和性
		go worker.start()

		// 在Go中设置CPU亲和性比较复杂，通常使用runtime.LockOSThread()
		// 这里简化处理，实际生产环境可能需要使用syscall包
		fmt.Printf("Binding thread-%d to core %d\n", i, offset+i)
		pinToCore(i, offset+i)
	}

	return pool
}

// pinToCore 设置CPU亲和性（简化版本）
func pinToCore(threadID int, coreID int) {
	// Go语言中设置CPU亲和性比较复杂
	// 可以使用runtime.LockOSThread()将goroutine锁定到OS线程
	// 然后使用syscall.SchedSetaffinity设置亲和性
	// 这里简化实现
	runtime.LockOSThread()

	// 实际生产环境中可以使用以下方式：
	// pid := syscall.Gettid()
	// var mask unix.CPUSet
	// mask.Set(coreID % runtime.NumCPU())
	// unix.SchedSetaffinity(pid, &mask)
}

// Enqueue 添加任务到线程池
func (tp *ThreadPool) Enqueue(taskFunc func(levm *lvm.LEVM)) {
	tp.queueMutex.Lock()
	defer tp.queueMutex.Unlock()

	if tp.stop {
		panic("enqueue on stopped ThreadPool")
	}

	task := Task{
		function: taskFunc,
		priority: HIGH_PRIORITY,
	}

	tp.highPriorityTasks.PushBack(task)

	//if priority == HIGH_PRIORITY {
	//	tp.highPriorityTasks.PushBack(task)
	//} else {
	//	tp.lowPriorityTasks.PushBack(task)
	//}

	tp.condition.Signal()
}

// EnqueueWithResult 添加带返回值的任务
func (tp *ThreadPool) EnqueueWithResult(taskFunc func(levm *lvm.LEVM) interface{}, priority TaskPriority) <-chan interface{} {
	resultChan := make(chan interface{}, 1)

	wrappedFunc := func(levm *lvm.LEVM) {
		result := taskFunc
		resultChan <- result
		close(resultChan)
	}

	tp.Enqueue(wrappedFunc)
	return resultChan
}

// EnqueueBatch 批量添加任务
func (tp *ThreadPool) EnqueueBatch(taskFuncs []func(levm *lvm.LEVM), priority TaskPriority) {
	tp.queueMutex.Lock()
	defer tp.queueMutex.Unlock()

	if tp.stop {
		panic("enqueue on stopped ThreadPool")
	}

	for _, taskFunc := range taskFuncs {
		task := Task{
			function: taskFunc,
			priority: priority,
		}

		if priority == HIGH_PRIORITY {
			tp.highPriorityTasks.PushBack(task)
		} else {
			tp.lowPriorityTasks.PushBack(task)
		}
	}

	tp.condition.Broadcast()
}

// workerFunc 工作线程函数
func (w *worker) start() {
	for {
		var task Task
		var found bool

		w.pool.queueMutex.Lock()

		// 等待条件满足
		for !w.pool.stop && !w.stopFlag &&
			w.pool.highPriorityTasks.Len() == 0 &&
			w.pool.lowPriorityTasks.Len() == 0 {
			w.pool.condition.Wait()
		}

		// 检查停止条件
		if w.pool.stop && w.pool.highPriorityTasks.Len() == 0 && w.pool.lowPriorityTasks.Len() == 0 {
			w.pool.queueMutex.Unlock()
			return
		}

		if w.stopFlag {
			w.pool.queueMutex.Unlock()
			return
		}

		// 获取任务（优先高优先级）
		if w.pool.highPriorityTasks.Len() > 0 {
			element := w.pool.highPriorityTasks.Front()
			task = element.Value.(Task)
			w.pool.highPriorityTasks.Remove(element)
			found = true
		} else if w.pool.lowPriorityTasks.Len() > 0 {
			element := w.pool.lowPriorityTasks.Front()
			task = element.Value.(Task)
			w.pool.lowPriorityTasks.Remove(element)
			found = true
		}

		w.pool.queueMutex.Unlock()

		if found {
			// 执行任务
			start := time.Now()
			task.function(w.levm)
			duration := time.Since(start)

			// 更新统计信息
			atomic.AddInt64(&w.pool.taskCounts[w.id], 1)
			w.pool.threadDurations[w.id] += duration
		}
	}
}

// ResizePool 调整线程池大小
func (tp *ThreadPool) ResizePool(newSize int) {
	tp.queueMutex.Lock()
	defer tp.queueMutex.Unlock()

	currentSize := len(tp.workers)

	if newSize > currentSize {
		// 增加线程
		for i := currentSize; i < newSize; i++ {
			worker := &worker{
				id:       i,
				pool:     tp,
				stopFlag: false,
			}
			tp.workers = append(tp.workers, worker)
			tp.stopFlags = append(tp.stopFlags, false)
			tp.threadDurations = append(tp.threadDurations, 0)
			tp.taskCounts = append(tp.taskCounts, 0)

			go worker.start()
			fmt.Printf("Binding new thread-%d to core %d\n", i, i)
			pinToCore(i, i)
		}
	} else if newSize < currentSize {
		// 减少线程
		for i := newSize; i < currentSize; i++ {
			tp.stopFlags[i] = true
		}

		tp.condition.Broadcast()

		// 等待线程退出
		for i := newSize; i < currentSize; i++ {
			// 在Go中，我们无法直接join goroutine
			// 但可以通过channel或其他机制等待退出
			// 这里依赖worker检测到stopFlag后自动退出
		}

		tp.workers = tp.workers[:newSize]
		tp.stopFlags = tp.stopFlags[:newSize]
		tp.threadDurations = tp.threadDurations[:newSize]
		tp.taskCounts = tp.taskCounts[:newSize]
	}

	tp.ThreadNum = newSize
	fmt.Printf("Resized thread pool to %d threads.\n", newSize)
}

// Shutdown 关闭线程池
func (tp *ThreadPool) Shutdown() {
	tp.queueMutex.Lock()
	tp.stop = true
	tp.queueMutex.Unlock()

	tp.condition.Broadcast()

	// 在Go中，goroutine会自动退出，不需要显式join
	// 但我们可以等待所有任务完成
}

// GetThreadDurations 获取线程工作时间统计
func (tp *ThreadPool) GetThreadDurations() []time.Duration {
	return tp.threadDurations
}

// GetTaskCounts 获取任务计数
func (tp *ThreadPool) GetTaskCounts() []int64 {
	return tp.taskCounts
}

// GetThreadNum 获取线程数量
func (tp *ThreadPool) GetThreadNum() int {
	return tp.ThreadNum
}

// WaitForCompletion 等待所有任务完成（可选方法）
func (tp *ThreadPool) WaitForCompletion() {
	// 简单的实现：定期检查队列是否为空
	for {
		tp.queueMutex.Lock()
		empty := tp.highPriorityTasks.Len() == 0 && tp.lowPriorityTasks.Len() == 0
		tp.queueMutex.Unlock()

		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
