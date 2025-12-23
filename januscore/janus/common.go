package janus

import (
	"sync"
)

func waitHere(workerID int, mu *sync.Mutex, cond *sync.Cond, done *bool) (isWait bool) {
	mu.Lock()
	for !(*done) {
		isWait = true
		cond.Wait()
	}
	mu.Unlock()
	return isWait
}
