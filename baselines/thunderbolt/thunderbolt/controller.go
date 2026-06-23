package thunderbolt

import (
	"fmt"
	"sort"
)

type keyDependencyState struct {
	latestWriter *thunderboltTransaction
	readers      map[*thunderboltTransaction]struct{}
}

type thunderboltController struct {
	keyStates  map[string]*keyDependencyState
	indegree   map[*thunderboltTransaction]int
	dependents map[*thunderboltTransaction][]*thunderboltTransaction
	seenEdges  map[*thunderboltTransaction]map[*thunderboltTransaction]struct{}
}

type thunderboltExecutionPlan struct {
	allTxs     []*thunderboltTransaction
	order      []*thunderboltTransaction
	indegree   map[*thunderboltTransaction]int
	dependents map[*thunderboltTransaction][]*thunderboltTransaction
	remaining  int
}

func buildThunderboltPlan(txs []*thunderboltTransaction) *thunderboltExecutionPlan {
	ordered := append([]*thunderboltTransaction(nil), txs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].preplayOrder == ordered[j].preplayOrder {
			return ordered[i].inner.Txid < ordered[j].inner.Txid
		}
		return ordered[i].preplayOrder < ordered[j].preplayOrder
	})

	controller := newThunderboltController(ordered)
	for _, tx := range ordered {
		controller.addTransaction(tx)
	}

	plan := &thunderboltExecutionPlan{
		allTxs:     ordered,
		indegree:   cloneIndegree(controller.indegree),
		dependents: cloneDependents(controller.dependents),
		remaining:  len(ordered),
	}
	plan.order = plan.topologicalOrder()
	return plan
}

func newThunderboltController(txs []*thunderboltTransaction) *thunderboltController {
	controller := &thunderboltController{
		keyStates:  make(map[string]*keyDependencyState),
		indegree:   make(map[*thunderboltTransaction]int, len(txs)),
		dependents: make(map[*thunderboltTransaction][]*thunderboltTransaction, len(txs)),
		seenEdges:  make(map[*thunderboltTransaction]map[*thunderboltTransaction]struct{}),
	}
	for _, tx := range txs {
		tx.refreshReadWriteSet()
		controller.indegree[tx] = 0
	}
	return controller
}

func (c *thunderboltController) addTransaction(tx *thunderboltTransaction) {
	for _, op := range tx.operations() {
		c.addOperation(op)
	}
}

func (c *thunderboltController) addOperation(op thunderboltOperation) {
	if op.tx == nil || op.key == "" {
		return
	}
	if op.opType == thunderboltRead {
		c.addRead(op.tx, op.key)
		return
	}
	c.addWrite(op.tx, op.key)
}

func (c *thunderboltController) addRead(tx *thunderboltTransaction, key string) {
	state := c.stateForKey(key)
	if state.latestWriter != nil {
		c.addDependency(state.latestWriter, tx)
	}
	state.readers[tx] = struct{}{}
}

func (c *thunderboltController) addWrite(tx *thunderboltTransaction, key string) {
	state := c.stateForKey(key)
	if state.latestWriter != nil {
		c.addDependency(state.latestWriter, tx)
	}
	for reader := range state.readers {
		c.addDependency(reader, tx)
	}
	state.latestWriter = tx
	state.readers = make(map[*thunderboltTransaction]struct{})
}

func (c *thunderboltController) stateForKey(key string) *keyDependencyState {
	state := c.keyStates[key]
	if state != nil {
		return state
	}
	state = &keyDependencyState{
		readers: make(map[*thunderboltTransaction]struct{}),
	}
	c.keyStates[key] = state
	return state
}

func (c *thunderboltController) addDependency(from, to *thunderboltTransaction) {
	if from == nil || to == nil || from == to {
		return
	}
	if c.seenEdges[from] == nil {
		c.seenEdges[from] = make(map[*thunderboltTransaction]struct{})
	}
	if _, exists := c.seenEdges[from][to]; exists {
		return
	}
	c.seenEdges[from][to] = struct{}{}
	c.dependents[from] = append(c.dependents[from], to)
	c.indegree[to]++
}

func (p *thunderboltExecutionPlan) topologicalOrder() []*thunderboltTransaction {
	indegree := cloneIndegree(p.indegree)
	ready := make([]*thunderboltTransaction, 0)
	for _, tx := range p.allTxs {
		if indegree[tx] == 0 {
			ready = append(ready, tx)
		}
	}

	order := make([]*thunderboltTransaction, 0, len(p.allTxs))
	for len(ready) > 0 {
		tx := ready[0]
		ready = ready[1:]
		order = append(order, tx)

		for _, dependent := range p.dependents[tx] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}

	if len(order) != len(p.allTxs) {
		panic(fmt.Sprintf("Thunderbolt dependency graph cycle: ordered=%d total=%d", len(order), len(p.allTxs)))
	}
	return order
}

func cloneIndegree(src map[*thunderboltTransaction]int) map[*thunderboltTransaction]int {
	dst := make(map[*thunderboltTransaction]int, len(src))
	for tx, degree := range src {
		dst[tx] = degree
	}
	return dst
}

func cloneDependents(src map[*thunderboltTransaction][]*thunderboltTransaction) map[*thunderboltTransaction][]*thunderboltTransaction {
	dst := make(map[*thunderboltTransaction][]*thunderboltTransaction, len(src))
	for tx, dependents := range src {
		dst[tx] = append([]*thunderboltTransaction(nil), dependents...)
	}
	return dst
}
