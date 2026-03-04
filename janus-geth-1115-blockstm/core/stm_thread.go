package core

import (
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/ethdb"
	"fmt"
	"log"
	"math/big"
)

// STMThread implements the STM thread for executing and validating txs
type STMThread struct {
	Scheduler *Scheduler
	MVMemory  *state.StmStateDB
	chainDb   ethdb.Database
}

// NewThread creates a new instance of STM thread
func NewThread(scheduler *Scheduler, memory *state.StmStateDB, chainDb ethdb.Database) *STMThread {
	return &STMThread{
		Scheduler: scheduler,
		MVMemory:  memory,
		chainDb:   chainDb,
	}
}

// Run starts a Block-STM thread
func (thread *STMThread) Run() {
	var task *Task

	for !thread.Scheduler.Done() {
		//if task != nil {
		//	fmt.Println("Tx.Index", task.StmTx.Tx.Index, "task.Kind", task.Kind)
		//}
		if task != nil && task.Kind == EXECUTION_T {
			//fmt.Println("EXECUTION_T", task.StmTx)
			var err error
			//fmt.Println("executing task", task.StmTx.Tx.Index)
			task, err = thread.tryExecute(task)
			if err != nil {
				log.Panic(err)
			}
		} else if task != nil && task.Kind == VALIDATION_T {
			var err error
			//fmt.Println("validating task", task.StmTx.Tx.Index)
			task, err = thread.tryValidate(task)
			if err != nil {
				log.Panic(err)
			}
		} else { // task is nil
			task = thread.Scheduler.NextTask()
		}
	}
}

// tryExecute conducts an execution task
func (thread *STMThread) tryExecute(task *Task) (*Task, error) {
	//fmt.Println("executing task", task.StmTx.Tx.Index)
	var (
		blockingTx int
		readSet    []*state.ReadLoc
		writeSet   state.WriteSets
		err        error
	)
	if thread.Scheduler.stmTxs != nil {
		blockingTx, readSet, writeSet, err = thread.processByLarge(task.StmTx, task.Ver, thread.MVMemory, vm.Config{EnablePreimageRecording: false})
	} else {
		blockingTx, readSet, writeSet, err = thread.processByBlock(task.Tx, task.Ver, thread.MVMemory, vm.Config{EnablePreimageRecording: false})
	}

	if err != nil {
		return nil, err
	}
	//fmt.Println("tx index", task.StmTx.Tx.Index, "blocking tx", blockingTx)
	if blockingTx > -1 {
		thread.Scheduler.reExecutionCounter.Add(1)
		success, err2 := thread.Scheduler.AddDependency(task.Ver.Index, blockingTx)
		if err2 != nil {
			return nil, err2
		}
		if !success {
			return thread.tryExecute(task)
		}
		return nil, nil
	}
	//fmt.Println("task.StmTx.Tx.Index, readSet", task.StmTx.Tx.Index, readSet)
	newWrites := thread.MVMemory.Record(task.Ver, readSet, writeSet)
	return thread.Scheduler.FinishExecution(task.Ver.Index, task.Ver.Incarnation, newWrites)
}

// process executes a specific tx in VM and obtains the fetched read/write set
func (thread *STMThread) processByBlock(tx *types.Transaction, version *state.TxInfoMini, stmStateDB *state.StmStateDB, cfg vm.Config) (int, []*state.ReadLoc, state.WriteSets, error) {
	var (
		usedGas     = thread.Scheduler.blockInfo.UsedGas
		header      = thread.Scheduler.blockInfo.Header
		blockHash   = thread.Scheduler.blockInfo.BlockHash
		blockNumber = thread.Scheduler.blockInfo.BlockNumber
		gp          = thread.Scheduler.blockInfo.GasPool.Copy()
		context     = thread.Scheduler.context
		chainConfig = thread.Scheduler.config
	)

	stmTxDB := state.NewStmTransaction(tx, version.Index, version.Incarnation, stmStateDB)
	vmenv := vm.NewEVM(context, vm.TxContext{}, stmTxDB, chainConfig, cfg)
	msg, err := TransactionToMessage(tx, types.MakeSigner(chainConfig, header.Number), header.BaseFee)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("could not apply tx %d [%v]: %w", version.Index, tx.Hash().Hex(), err)
	}
	// 避免Nonce错误
	stmTxDB.SetNonce(msg.From, msg.Nonce)
	// 避免Balance错误
	mgval := new(big.Int).SetUint64(msg.GasLimit)
	mgval = mgval.Mul(mgval, msg.GasPrice)
	balanceCheck := mgval
	if msg.GasFeeCap != nil {
		balanceCheck = new(big.Int).SetUint64(msg.GasLimit)
		balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
		balanceCheck.Add(balanceCheck, msg.Value)
	}
	stmTxDB.AddBalance(msg.From, balanceCheck)
	//
	//// 避免Transfer错误
	//newData, ok := setting.IsERCTransfer(msg.Data)
	//if ok {
	//	msg.Data = common.CopyBytes(newData)
	//}

	receipt, err := applyStmTransaction(msg, chainConfig, gp, stmStateDB, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("could not apply tx %d [%v]: %w", version.Index, tx.Hash().Hex(), err)
	}

	blockingTx, readSet, writeSet := stmTxDB.OutputRWSet()

	if blockingTx > -1 {
		return blockingTx, nil, nil, nil
	}

	thread.Scheduler.logMutex.Lock()
	thread.Scheduler.blockInfo.Receipts = append(thread.Scheduler.blockInfo.Receipts, receipt)
	thread.Scheduler.blockInfo.AllLogs = append(thread.Scheduler.blockInfo.AllLogs, receipt.Logs...)
	thread.Scheduler.logMutex.Unlock()
	return blockingTx, readSet, writeSet, nil
}

// process executes a specific tx in VM and obtains the fetched read/write set
func (thread *STMThread) processByLarge(tx *types.BlockStmTx, version *state.TxInfoMini, stmStateDB *state.StmStateDB, cfg vm.Config) (int, []*state.ReadLoc, state.WriteSets, error) {
	//fmt.Println("processByLarge", tx)
	var (
		usedGas     = new(uint64)
		header      = tx.Block.Header()
		blockHash   = tx.Block.Hash()
		blockNumber = tx.Block.Number()
		gp          = new(GasPool).AddGas(tx.Block.GasLimit())
		context     = NewEVMBlockContext(header, thread.chainDb, nil)
		chainConfig = thread.Scheduler.config
	)

	stmTxDB := state.NewStmTransaction(tx.Tx, version.Index, version.Incarnation, stmStateDB)
	vmenv := vm.NewEVM(context, vm.TxContext{}, stmTxDB, chainConfig, cfg)
	msg, err := TransactionToMessage(tx.Tx, types.MakeSigner(chainConfig, header.Number), header.BaseFee)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("could not apply tx %d [%v]: %w", version.Index, tx.Tx.Hash().Hex(), err)
	}
	// 避免Nonce错误
	stmTxDB.SetNonce(msg.From, msg.Nonce)
	// 避免Balance错误
	mgval := new(big.Int).SetUint64(msg.GasLimit)
	mgval = mgval.Mul(mgval, msg.GasPrice)
	balanceCheck := mgval
	if msg.GasFeeCap != nil {
		balanceCheck = new(big.Int).SetUint64(msg.GasLimit)
		balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
		balanceCheck.Add(balanceCheck, msg.Value)
	}
	stmTxDB.AddBalance(msg.From, balanceCheck)
	//
	//// 避免Transfer错误
	//newData, ok := setting.IsERCTransfer(msg.Data)
	//if ok {
	//	msg.Data = common.CopyBytes(newData)
	//}

	_, err = applyStmTransaction(msg, chainConfig, gp, stmStateDB, stmTxDB, blockNumber, blockHash, tx.Tx, usedGas, vmenv)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("could not apply tx %d [%v]: %w", version.Index, tx.Tx.Hash().Hex(), err)
	}

	blockingTx, readSet, writeSet := stmTxDB.OutputRWSet()

	if blockingTx > -1 {
		return blockingTx, nil, nil, nil
	}

	thread.Scheduler.logMutex.Lock()
	//thread.Scheduler.blockInfo.Receipts = append(thread.Scheduler.blockInfo.Receipts, receipt)
	//thread.Scheduler.blockInfo.AllLogs = append(thread.Scheduler.blockInfo.AllLogs, receipt.Logs...)
	thread.Scheduler.logMutex.Unlock()
	return blockingTx, readSet, writeSet, nil
}

// tryValidate conducts a validation task
func (thread *STMThread) tryValidate(task *Task) (*Task, error) {
	//fmt.Println(task.Ver.Index)
	isValid := thread.MVMemory.ValidateReadSet(task.Ver.Index)
	canAbort := !isValid && thread.Scheduler.TryAbort(task.Ver.Index, task.Ver.Incarnation)
	if canAbort {
		//fmt.Println("tryValidate abort tx", task.Ver.Index, "incarnation", task.Ver.Incarnation)
		thread.MVMemory.MarkEstimates(task.Ver.Index)
		thread.Scheduler.reExecutionCounter.Add(1)
	}
	task2, err := thread.Scheduler.FinishValidation(task.Ver.Index, canAbort)
	//if task2 == nil {
	//	fmt.Printf("index: %d committed, isAbort: %v\n", task.Ver.Index, canAbort)
	//}
	return task2, err
}
