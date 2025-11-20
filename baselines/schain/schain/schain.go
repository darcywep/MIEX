package schain

import (
	"Janus/config"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"runtime"
	"sync"

	"github.com/holiman/uint256"
)

type peepTx struct {
	tx             *types.Transaction
	writeKeys      map[string]struct{}
	readKeys       map[string]struct{}
	needLockNumber int
	index          int // 在区块里的下标
}

type accessSequence struct {
	tx       *peepTx
	readOnly bool
}

type addressAccessSequence struct {
	sequence  []accessSequence
	lockIndex int  // 锁目前所在的位置
	isWrite   bool // 当前是否授予了写锁
}

func TestSerialExecution(txs []*types.Transaction, levm *lvm.LEVM) {
	newLevm := levm.Copy()
	tools.CatStorageState = true
	for _, tx := range txs {
		//fmt.Println(common.Bytes2Hex(tx.Data()))
		//fmt.Println("newLevm.AllDB().StateDB.GetBalance(*tx.From())", *tx.From(), *tx.To(), newLevm.AllDB().StateDB.GetBalance(*tx.From()))
		_, err := newLevm.CallContractUseStateDB(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0), levm.AllDB().StateDB)
		tools.PanicError("SChain TestSerialExecution", err)
		tx.WriteKeys = make([]string, 0)
		tx.ReadKeys = make([]string, 0)
		if tx.TxType == config.IOTx {
			tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
			tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
		} else {
			tx.WriteKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
		}
	}
}

func GetRWSetByOCC(txs []*types.Transaction, levm *lvm.LEVM) {
	txsChan := make(chan *types.Transaction, len(txs))
	wg := &sync.WaitGroup{}
	wg.Add(janusConfig.AllThreadNum + 1)
	go func() {
		runtime.LockOSThread()
		defer wg.Done()
		for _, tx := range txs {
			txsChan <- tx
		}
		close(txsChan)
	}()
	//tools.CatStorageState = true
	newLevms := make([]*lvm.LEVM, 0, janusConfig.AllThreadNum)
	for i := 0; i < janusConfig.AllThreadNum; i++ {
		newLevms = append(newLevms, levm.Copy())
	}
	for i := 0; i < janusConfig.AllThreadNum; i++ {
		newLevm := newLevms[i]
		go func() {
			runtime.LockOSThread()
			defer wg.Done()
			for tx := range txsChan {
				//fmt.Println(common.Bytes2Hex(tx.Data()))
				_, err := newLevm.CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
				tools.PanicError("SChain GetRWSetByOCC Execute", err)
				tx.WriteKeys = make([]string, 0)
				tx.ReadKeys = make([]string, 0)
				//tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
				//tx.ReadKeys = append(tx.ReadKeys, tx.From().String())
				//tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
				//tx.ReadKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
				if tx.TxType == config.IOTx {
					tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
					tx.ReadKeys = append(tx.ReadKeys, tx.From().String())
					tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
					tx.ReadKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
				} else {
					tx.ReadKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
					tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
				}
			}
		}()
	}
	wg.Wait()
}

func SChain(txs []*types.Transaction, levm *lvm.LEVM) {
	stateAccessSequence, inactivePeepTxs := constructStateAccessSequence(txs)

	for {
		newInactivePeepTxs := make([]*peepTx, 0)
		activePeepTxs := make([]*peepTx, 0)
		activePeepTxsChan := make(chan *types.Transaction, len(txs))
		for _, inactiveTx := range inactivePeepTxs {
			notGetLocksNum := grantLock(stateAccessSequence, inactiveTx)
			if notGetLocksNum == 0 {
				activePeepTxs = append(activePeepTxs, inactiveTx)
				activePeepTxsChan <- inactiveTx.tx
			} else {
				newInactivePeepTxs = append(newInactivePeepTxs, inactiveTx)
			}
		}
		close(activePeepTxsChan)
		wg := new(sync.WaitGroup)
		wg.Add(config.AllThreadNum)
		newLevms := make([]*lvm.LEVM, 0, janusConfig.AllThreadNum)
		for i := 0; i < janusConfig.AllThreadNum; i++ {
			newLevms = append(newLevms, levm.Copy())
		}
		for k := 0; k < config.AllThreadNum; k++ {
			go runTx(wg, newLevms[k], activePeepTxsChan, nil)
		}
		wg.Wait()
		for i := 0; i < janusConfig.AllThreadNum; i++ {
			newLevms[i].AllDB().StateDB.FlushDirtyToNewStateDB(levm.AllDB().StateDB)
		}

		for _, tx := range activePeepTxs {
			releaseLock(stateAccessSequence, tx)
		}

		if len(newInactivePeepTxs) == 0 { // 没有待执行的交易了
			break
		}
		inactivePeepTxs = newInactivePeepTxs
	}

}

func SChainParallelUp(txs []*types.Transaction, levm *lvm.LEVM) {
	stateAccessSequence, inactivePeepTxs := constructStateAccessSequence(txs)
	activePeepTxsChan := make(chan *types.Transaction, len(txs))
	finishExecutionSignalChan := make(chan struct{}, len(txs))
	wg := new(sync.WaitGroup)
	wg.Add(config.AllThreadNum)
	go func() {
		runtime.LockOSThread()
		defer wg.Done()
		txSum := len(txs)
		finishTxSum := 0
		for {
			newInactivePeepTxs := make([]*peepTx, 0)
			activePeepTxs := make([]*peepTx, 0)
			for _, inactiveTx := range inactivePeepTxs {
				notGetLocksNum := grantLock(stateAccessSequence, inactiveTx)
				if notGetLocksNum == 0 {
					activePeepTxs = append(activePeepTxs, inactiveTx)
					activePeepTxsChan <- inactiveTx.tx
				} else {
					newInactivePeepTxs = append(newInactivePeepTxs, inactiveTx)
				}
			}
			//fmt.Println("activePeepTxs:", len(activePeepTxs), "inactivePeepTxs:", len(inactivePeepTxs))
			//fmt.Println("finishTxSum:", finishTxSum, "txSum:", txSum)
			//time.Sleep(1 * time.Second)
			finishTxNumber := 0
			for _ = range finishExecutionSignalChan {
				finishTxNumber++
				finishTxSum++
				if finishTxNumber == len(activePeepTxs) {
					break
				}
			}
			if finishTxSum == txSum {
				close(activePeepTxsChan)
				close(finishExecutionSignalChan)
			}

			for _, tx := range activePeepTxs {
				releaseLock(stateAccessSequence, tx)
			}

			if len(newInactivePeepTxs) == 0 { // 没有待执行的交易了
				break
			}
			inactivePeepTxs = newInactivePeepTxs
		}
	}()

	newLevms := make([]*lvm.LEVM, 0, janusConfig.AllThreadNum-1)
	for i := 0; i < janusConfig.AllThreadNum-1; i++ {
		newLevms = append(newLevms, levm.Copy())
	}
	for k := 0; k < config.AllThreadNum-1; k++ {
		go runTx(wg, newLevms[k], activePeepTxsChan, finishExecutionSignalChan)
	}
	wg.Wait()
	for i := 0; i < janusConfig.AllThreadNum-1; i++ {
		newLevms[i].AllDB().StateDB.FlushDirtyToNewStateDB(levm.AllDB().StateDB)
	}
}

func constructStateAccessSequence(txs []*types.Transaction) (stateAccessSequence map[string]*addressAccessSequence, peepTxs []*peepTx) {
	stateAccessSequence = make(map[string]*addressAccessSequence)
	peepTxs = make([]*peepTx, 0)
	for i, tx := range txs { // 先统计出读写集
		writeKeys := make(map[string]struct{})
		readKeys := make(map[string]struct{})
		for _, key := range tx.WriteKeys {
			if _, ok := writeKeys[key]; !ok {
				writeKeys[key] = struct{}{}
			}
		}

		for _, key := range tx.ReadKeys {
			_, okw := writeKeys[key]
			_, okr := readKeys[key]
			if !okw && !okr {
				readKeys[key] = struct{}{}
			}
		}

		peepTxs = append(peepTxs, &peepTx{
			tx:             tx,
			writeKeys:      writeKeys,
			readKeys:       readKeys,
			needLockNumber: len(writeKeys) + len(readKeys),
			index:          i,
		})
	}

	for _, tx := range peepTxs { // 构建访问序列、
		for key, _ := range tx.writeKeys {
			if _, ok := stateAccessSequence[key]; !ok {
				stateAccessSequence[key] = &addressAccessSequence{
					sequence:  make([]accessSequence, 0),
					lockIndex: 0,
					isWrite:   false,
				}
			}
			stateAccessSequence[key].sequence = append(stateAccessSequence[key].sequence, accessSequence{tx: tx, readOnly: false})
		}
		for key, _ := range tx.readKeys {
			if _, ok := stateAccessSequence[key]; !ok {
				stateAccessSequence[key] = &addressAccessSequence{
					sequence:  make([]accessSequence, 0),
					lockIndex: 0,
					isWrite:   false,
				}
			}
			stateAccessSequence[key].sequence = append(stateAccessSequence[key].sequence, accessSequence{tx: tx, readOnly: false})
		}
	}

	return stateAccessSequence, peepTxs
}

func grantLock(stateAccessSequence map[string]*addressAccessSequence, tx *peepTx) int {
	lockNumber := 0
	var lock sync.Mutex
	// 询问锁并授予锁
	for key, _ := range tx.writeKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if !sequence.isWrite && sequence.sequence[sequence.lockIndex].tx.index == tx.index {
			lockNumber++
			sequence.lockIndex++
			sequence.isWrite = true
			lock.Lock()
			lock.Unlock()
		}
	}
	for key, _ := range tx.readKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if !sequence.isWrite && sequence.sequence[sequence.lockIndex].tx.index == tx.index {
			lockNumber++
			sequence.lockIndex++
		}
	}

	if tx.needLockNumber-lockNumber == 0 { // 拿到所有的锁
		return tx.needLockNumber - lockNumber
	}
	// 未拿到所有的锁，返还

	for key, _ := range tx.writeKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if sequence.isWrite && sequence.sequence[sequence.lockIndex-1].tx.index == tx.index {
			sequence.isWrite = false
			sequence.lockIndex--
		}
	}
	for key, _ := range tx.readKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if !sequence.isWrite && sequence.sequence[sequence.lockIndex-1].tx.index == tx.index {
			sequence.lockIndex--
		}
	}
	return tx.needLockNumber - lockNumber
}

func releaseLock(stateAccessSequence map[string]*addressAccessSequence, tx *peepTx) {
	for key, _ := range tx.writeKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if sequence.isWrite {
			sequence.isWrite = false
		}
	}
}

func runTx(wg *sync.WaitGroup, levm *lvm.LEVM, activePeepTxsChan chan *types.Transaction, finishExecutionSignal chan struct{}) {
	defer wg.Done()
	runtime.LockOSThread()
	for tx := range activePeepTxsChan {
		_, err := levm.CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("SChain Tx Execute", err)
		tx.WriteKeys = make([]string, 0)
		tx.ReadKeys = make([]string, 0)
		if tx.TxType == config.IOTx {
			tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
			tx.WriteKeys = append(tx.WriteKeys, tx.To().String())
		} else {
			tx.WriteKeys = append(tx.ReadKeys, tx.To().String())
		}
		if finishExecutionSignal != nil {
			finishExecutionSignal <- struct{}{}
		}
	}
}
