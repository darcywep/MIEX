package schain

import (
	"Janus/config"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/state"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"math/big"
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

func GetRWSetByOCC(txs []*types.Transaction, statedb *state.StateDB) {
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
	for i := 0; i < janusConfig.AllThreadNum; i++ {
		newStateDB := statedb.Copy()
		go func() {
			runtime.LockOSThread()
			defer wg.Done()
			lvm := lvm.LEVM{}
			blockNumber := new(big.Int).SetUint64(1)
			lvm.NewEVM(blockNumber, tools.GenerateAddress())
			for tx := range txsChan {
				_, err := lvm.CallContractUseStateDB(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0), newStateDB)
				tools.PanicError(err)
				tx.WriteKeys = make([]string, 0)
				tx.ReadKeys = make([]string, 0)
				if tx.TxType == config.IOTx {
					tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
					tx.WriteKeys = append(tx.WriteKeys, tx.To().String())
				} else {
					tx.WriteKeys = append(tx.ReadKeys, tx.To().String())
				}
			}
		}()
	}
	wg.Wait()
}

func SChain(txs []*types.Transaction, statedb *state.StateDB) {
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
		for k := 0; k < config.AllThreadNum; k++ {
			newStateDB := statedb.Copy()
			go runTx(wg, newStateDB, activePeepTxsChan)
			newStateDB.FlushDirtyToNewStateDB(statedb)
		}
		wg.Wait()
		for _, tx := range activePeepTxs {
			releaseLock(stateAccessSequence, tx)
		}

		if len(newInactivePeepTxs) == 0 { // 没有待执行的交易了
			break
		}
		inactivePeepTxs = newInactivePeepTxs
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
	for key, _ := range tx.writeKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if !sequence.isWrite && sequence.sequence[sequence.lockIndex].tx.index == tx.index {
			lockNumber++
			sequence.isWrite = true
			sequence.lockIndex++
			lock.Lock()
			lock.Unlock()
		}
	}
	for key, _ := range tx.readKeys {
		sequence, ok := stateAccessSequence[key]
		if !ok {
			panic("peep: key in the sequence is not found")
		}
		if !sequence.isWrite {
			lockNumber++
			sequence.lockIndex++
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

func runTx(wg *sync.WaitGroup, statedb *state.StateDB, activePeepTxsChan chan *types.Transaction) {
	runtime.LockOSThread()
	defer wg.Done()
	lvm := lvm.LEVM{}
	blockNumber := new(big.Int).SetUint64(1)
	lvm.NewEVM(blockNumber, tools.GenerateAddress())
	for tx := range activePeepTxsChan {
		_, err := lvm.CallContractUseStateDB(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0), statedb)
		tools.PanicError(err)
		tx.WriteKeys = make([]string, 0)
		tx.ReadKeys = make([]string, 0)
		if tx.TxType == config.IOTx {
			tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
			tx.WriteKeys = append(tx.WriteKeys, tx.To().String())
		} else {
			tx.WriteKeys = append(tx.ReadKeys, tx.To().String())
		}
	}
}
