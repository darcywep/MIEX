package core

import (
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/ethdb"
	"errors"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

// Scheduler implements the Block-STM scheduler for scheduling execution and validation tasks
type Scheduler struct {
	deps               *dependencies
	stmTxs             types.BlockStmTxs
	txStatus           *txStatus
	executionID        atomic.Int64
	validationID       atomic.Int64
	decreaseCounter    atomic.Int64
	activeTasks        atomic.Int64
	reExecutionCounter atomic.Int64
	block              *types.Block
	txNum              int
	blockInfo          *BlockInfo
	context            vm.BlockContext
	config             *params.ChainConfig
	doneMarker         bool
	logMutex           sync.Mutex
}

type BlockInfo struct {
	Receipts    types.Receipts
	UsedGas     *uint64
	Header      *types.Header
	BlockHash   common.Hash
	BlockNumber *big.Int
	AllLogs     []*types.Log
	GasPool     *GasPool
}

func newBlockInfo(block *types.Block) *BlockInfo {
	return &BlockInfo{
		UsedGas:     new(uint64),
		Header:      block.Header(),
		BlockHash:   block.Hash(),
		BlockNumber: block.Number(),
		GasPool:     new(GasPool).AddGas(1000 * block.GasLimit()),
	}
}

type Task struct {
	Tx    *types.Transaction
	StmTx *types.BlockStmTx
	Kind  int
	Ver   *state.TxInfoMini
}

type dependencies struct {
	dataMap map[int][]int
	mapLock sync.RWMutex
}

type txStatus struct {
	dataMap map[int]*status
	mapLock sync.RWMutex
}

type status struct {
	incarnation int
	curStatus   int
}

const (
	READY = iota
	EXECUTING
	EXECUTED
	ABORTING
)

const (
	EXECUTION_T = iota
	VALIDATION_T
)

// NewScheduler initializes a new Block-STM scheduler instance
func NewScheduler(block *types.Block, txs types.BlockStmTxs, config *params.ChainConfig, chainDb ethdb.Database) *Scheduler {
	depsMap := &dependencies{dataMap: make(map[int][]int)}
	statusMap := &txStatus{dataMap: make(map[int]*status)}
	var blockInfo *BlockInfo
	var context vm.BlockContext
	var txNum int
	if block != nil {
		for i := 0; i < len(block.Transactions()); i++ {
			depsMap.dataMap[i] = []int{}
			statusMap.dataMap[i] = &status{incarnation: 1, curStatus: READY}
		}
		blockInfo = newBlockInfo(block)
		context = NewEVMBlockContext(blockInfo.Header, chainDb, nil)
		txNum = len(block.Transactions())
	} else {
		for i := 0; i < len(txs); i++ {
			depsMap.dataMap[i] = []int{}
			statusMap.dataMap[i] = &status{incarnation: 1, curStatus: READY}
		}
		txNum = len(txs)
	}
	context.BaseFee = big.NewInt(1)
	s := &Scheduler{
		deps:       depsMap,
		txStatus:   statusMap,
		block:      block,
		stmTxs:     txs,
		txNum:      txNum,
		blockInfo:  blockInfo,
		context:    context,
		config:     config,
		doneMarker: false,
	}
	//fmt.Println("s.executionID.Load(), s.validationID.Load()", s.executionID.Load(), s.validationID.Load())

	return s
}

// GetBlock gets the processing block
func (sc *Scheduler) GetBlock() *types.Block { return sc.block }

// GetBlockInfo gets the block information
func (sc *Scheduler) GetBlockInfo() *BlockInfo { return sc.blockInfo }

// NextTask generates a new execution or validation task for next tx
func (sc *Scheduler) NextTask() *Task {
	if sc.validationID.Load() < sc.executionID.Load() {
		//fmt.Println("sc.validationID.Load() < sc.executionID.Load()", sc.validationID.Load(), sc.executionID.Load())
		valVer := sc.nextValidate()
		if valVer != nil {
			if sc.stmTxs != nil {
				tx := sc.stmTxs[valVer.Index]
				return &Task{Tx: nil, StmTx: tx, Kind: VALIDATION_T, Ver: valVer}
			} else {
				tx := sc.block.Transactions()[valVer.Index]
				return &Task{Tx: tx, StmTx: nil, Kind: VALIDATION_T, Ver: valVer}
			}
		}
	} else {
		exeVer := sc.nextExecute()
		if exeVer != nil {
			//fmt.Println("sc.stmTxs: ", &sc.stmTxs)
			if sc.stmTxs != nil {
				tx := sc.stmTxs[exeVer.Index]
				//fmt.Println("sc.stmTxs[exeVer.Index]", tx)
				return &Task{Tx: nil, StmTx: tx, Kind: EXECUTION_T, Ver: exeVer}
			} else {
				tx := sc.block.Transactions()[exeVer.Index]
				return &Task{Tx: tx, StmTx: nil, Kind: EXECUTION_T, Ver: exeVer}
			}
		}
	}
	return nil
}

// FinishExecution finishes the current incarnation of a tx and returns the validation task if necessary
func (sc *Scheduler) FinishExecution(index, incarnation int, newWrites bool) (*Task, error) {
	sc.deps.mapLock.Lock()
	sc.txStatus.mapLock.Lock()
	defer sc.deps.mapLock.Unlock()
	defer sc.txStatus.mapLock.Unlock()

	if sc.txStatus.dataMap[index].curStatus == EXECUTING {
		sc.txStatus.dataMap[index].curStatus = EXECUTED
	} else {
		return nil, errors.New("wrong transaction status")
	}

	deps := sc.deps.dataMap[index]
	if len(deps) > 0 {
		sc.deps.dataMap[index] = []int{}
		if err := sc.resumeDependency(deps); err != nil {
			return nil, err
		}
	}

	if sc.validationID.Load() > int64(index) {
		if newWrites {
			// validates all transactions >= index
			sc.decreaseValidationID(index)
		} else {
			// only validates this tx
			var (
				tx    *types.Transaction
				stmTx *types.BlockStmTx
			)
			if sc.stmTxs != nil {
				stmTx = sc.stmTxs[index]
			} else {
				tx = sc.block.Transactions()[index]
			}
			newValTask := &Task{
				Tx:    tx,
				StmTx: stmTx,
				Kind:  VALIDATION_T,
				Ver: &state.TxInfoMini{
					Index:       index,
					Incarnation: incarnation,
				},
			}
			return newValTask, nil
		}
	}

	sc.activeTasks.Add(-1)
	return nil, nil
}

// FinishValidation finishes the current validation of a tx and returns the new re-execution task if necessary
func (sc *Scheduler) FinishValidation(index int, isAborted bool) (*Task, error) {
	if isAborted {
		sc.txStatus.mapLock.Lock()
		defer sc.txStatus.mapLock.Unlock()

		if err := sc.setReady(index); err != nil {
			return nil, err
		}
		sc.decreaseValidationID(index + 1)

		// do not miss the current re-execution task of this tx
		var (
			tx    *types.Transaction
			stmTx *types.BlockStmTx
		)
		if sc.stmTxs != nil {
			stmTx = sc.stmTxs[index]
		} else {
			tx = sc.block.Transactions()[index]
		}
		if sc.executionID.Load() > int64(index) {
			newVer := sc.tryIncarnate(index)
			if newVer != nil {
				newExeTask := &Task{
					Tx:    tx,
					StmTx: stmTx,
					Kind:  EXECUTION_T,
					Ver:   newVer,
				}
				return newExeTask, nil
			}
		}
	}

	sc.activeTasks.Add(-1)
	return nil, nil
}

// AddDependency determines whether tx_i on which tx_j depends can be captured when it is aborted
func (sc *Scheduler) AddDependency(index, blocking int) (bool, error) {
	sc.deps.mapLock.Lock()
	sc.txStatus.mapLock.Lock()
	defer sc.deps.mapLock.Unlock()
	defer sc.txStatus.mapLock.Unlock()
	if sc.txStatus.dataMap[blocking].curStatus == EXECUTED {
		return false, nil
	}
	if sc.txStatus.dataMap[index].curStatus == EXECUTING {
		sc.txStatus.dataMap[index].curStatus = ABORTING
		sc.deps.dataMap[blocking] = append(sc.deps.dataMap[blocking], index)
		sc.activeTasks.Add(-1)
	} else {
		return false, errors.New("wrong transaction status")
	}
	return true, nil
}

// TryAbort ensures each incarnation of a tx can only be aborted once
func (sc *Scheduler) TryAbort(index, incarnation int) bool {
	sc.txStatus.mapLock.Lock()
	defer sc.txStatus.mapLock.Unlock()
	st := sc.txStatus.dataMap[index]
	if st.incarnation == incarnation && st.curStatus == EXECUTED {
		sc.txStatus.dataMap[index].curStatus = ABORTING
		return true
	}
	return false
}

// Done returns if all tasks have been done
func (sc *Scheduler) Done() bool {
	return sc.doneMarker
}

func (sc *Scheduler) GetExecutionCounter() int64 {
	return sc.reExecutionCounter.Load()
}

// tryIncarnate tries the next incarnation of a tx (if another thread has begun the new incarnation, then fails)
func (sc *Scheduler) tryIncarnate(index int) *state.TxInfoMini {
	if index < sc.txNum {
		if sc.txStatus.dataMap[index].curStatus == READY {
			sc.txStatus.dataMap[index].curStatus = EXECUTING
			incarnation := sc.txStatus.dataMap[index].incarnation
			ver := &state.TxInfoMini{Index: index, Incarnation: incarnation}
			return ver
		}
	}
	sc.activeTasks.Add(-1)
	return nil
}

// setReady sets the status of a tx to be 'ready for execution'
func (sc *Scheduler) setReady(index int) error {
	if sc.txStatus.dataMap[index].curStatus == ABORTING {
		sc.txStatus.dataMap[index].curStatus = READY
		sc.txStatus.dataMap[index].incarnation++
	} else {
		return errors.New("wrong transaction status")
	}
	return nil
}

// resumeDependency resumes the execution of all dependent txs of a tx
func (sc *Scheduler) resumeDependency(deps []int) error {
	for _, dep := range deps {
		if err := sc.setReady(dep); err != nil {
			return err
		}
	}
	sort.Ints(deps)
	minTx := deps[0]
	sc.decreaseExecutionID(minTx)
	return nil
}

// nextExecute fetches the next version of tx to be executed
func (sc *Scheduler) nextExecute() *state.TxInfoMini {
	if sc.executionID.Load() >= int64(sc.txNum) {
		sc.checkDone()
		return nil
	}

	sc.activeTasks.Add(1)
	nextExecuteIndex := int(sc.executionID.Load())
	sc.executionID.Add(1)
	sc.txStatus.mapLock.Lock()
	ver := sc.tryIncarnate(nextExecuteIndex)
	sc.txStatus.mapLock.Unlock()
	return ver
}

// nextValidate fetches the next version of tx to be validated
func (sc *Scheduler) nextValidate() *state.TxInfoMini {
	if sc.validationID.Load() >= int64(sc.txNum) {
		sc.checkDone()
		return nil
	}

	sc.activeTasks.Add(1)
	nextValidateIndex := int(sc.validationID.Load())
	sc.validationID.Add(1)

	if nextValidateIndex < sc.txNum {
		sc.txStatus.mapLock.RLock()
		st := sc.txStatus.dataMap[nextValidateIndex]
		sc.txStatus.mapLock.RUnlock()
		if st.curStatus == EXECUTED {
			ver := &state.TxInfoMini{Index: nextValidateIndex, Incarnation: st.incarnation}
			return ver
		}
	}
	// invalid validation task
	sc.activeTasks.Add(-1)
	return nil
}

// decreaseExecutionID sets the current execution ID to the specific one if it is larger than the specific one
func (sc *Scheduler) decreaseExecutionID(index int) {
	if sc.executionID.Load() > int64(index) {
		sc.executionID.Store(int64(index))
	}
	sc.decreaseCounter.Add(1)
}

// decreaseValidationID sets the current validation ID to the specific one if it is larger than the specific one
func (sc *Scheduler) decreaseValidationID(index int) {
	if sc.validationID.Load() > int64(index) {
		sc.validationID.Store(int64(index))
	}
	sc.decreaseCounter.Add(1)
}

// checkDone checks if all the execution and validation tasks have been completed
func (sc *Scheduler) checkDone() {
	var minTaskID int64
	observed := sc.decreaseCounter.Load()

	if sc.executionID.Load() < sc.validationID.Load() {
		minTaskID = sc.executionID.Load()
	} else {
		minTaskID = sc.validationID.Load()
	}

	// sleep is used to detect if validation or execution index decreases
	// from their observed values
	time.Sleep(time.Millisecond * 100)
	// the observed counter is used to detect some tasks that fail
	// in validation and back to the execution queue
	if minTaskID >= int64(sc.txNum) && sc.activeTasks.Load() == 0 && observed == sc.decreaseCounter.Load() {
		sc.doneMarker = true
	}
}
