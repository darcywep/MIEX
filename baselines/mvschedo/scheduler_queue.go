package mvschedo

import "sync"

type queuedOperation struct {
	tx   *MVSchedOTransaction
	op   ScheduledOperation
	done bool
}

type keyScheduleQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []queuedOperation
}

type ScheduleQueues struct {
	queues map[string]*keyScheduleQueue
}

func NewScheduleQueues(txs []*MVSchedOTransaction) *ScheduleQueues {
	queues := &ScheduleQueues{
		queues: make(map[string]*keyScheduleQueue),
	}

	for _, tx := range txs {
		for _, op := range tx.Ops {
			queue := queues.queueForKey(op.Key)
			queue.items = append(queue.items, queuedOperation{tx: tx, op: op})
		}
	}

	return queues
}

func (q *ScheduleQueues) WaitTurn(tx *MVSchedOTransaction, op ScheduledOperation) {
	queue := q.queues[op.Key]
	if queue == nil {
		return
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	for {
		blocked := false
		for _, item := range queue.items {
			if item.done || item.tx == tx || item.tx.Timestamp >= tx.Timestamp {
				continue
			}
			if item.op.conflictsWith(op) {
				blocked = true
				break
			}
		}
		if !blocked {
			return
		}
		queue.cond.Wait()
	}
}

func (q *ScheduleQueues) MarkDone(tx *MVSchedOTransaction, op ScheduledOperation) {
	q.mark(tx, op, true)
}

func (q *ScheduleQueues) FreeTransaction(tx *MVSchedOTransaction) {
	for _, op := range tx.Ops {
		q.mark(tx, op, false)
	}
}

func (q *ScheduleQueues) queueForKey(key string) *keyScheduleQueue {
	queue := q.queues[key]
	if queue != nil {
		return queue
	}

	queue = &keyScheduleQueue{}
	queue.cond = sync.NewCond(&queue.mu)
	q.queues[key] = queue
	return queue
}

func (q *ScheduleQueues) mark(tx *MVSchedOTransaction, op ScheduledOperation, matchOp bool) {
	queue := q.queues[op.Key]
	if queue == nil {
		return
	}

	queue.mu.Lock()
	for idx := range queue.items {
		item := &queue.items[idx]
		if item.tx != tx {
			continue
		}
		if matchOp && item.op.ID != op.ID {
			continue
		}
		item.done = true
	}
	queue.cond.Broadcast()
	queue.mu.Unlock()
}
