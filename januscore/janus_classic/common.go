package janusClassic

import (
	"sync/atomic"
)

//func waitHere(workerID int, mu *sync.Mutex, cond *sync.Cond, done *bool) (isWait bool) {
//	mu.Lock()
//	for !(*done) {
//		isWait = true
//		cond.Wait()
//	}
//	mu.Unlock()
//	return isWait
//}

var enableLog = true

var (
	committedTxsNum       atomic.Int32
	enableReExecutePhase1 = false
	enableReExecutePhase2 = false
)

func appendThreadRWSets(state *BatchState, jtx *janusTransaction, workerID int) {
	if state.ThreadRWSets[workerID] == nil {
		state.ThreadRWSets[workerID] = make([]*ReadWriteSet, 0)
	}
	state.ThreadRWSets[workerID] = append(state.ThreadRWSets[workerID], jtx.rwSet)
}

func waitHere(done *atomic.Bool) (isWait bool) {
	//mu.Lock()
	for !done.Load() {
		isWait = true
		//cond.Wait()
	}
	//mu.Unlock()
	return isWait
}
