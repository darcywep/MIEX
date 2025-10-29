package peep

import (
	"Janus/config"
	"Janus/mempool"
	"Janus/persister"
	"Janus/scheduler"
	"fmt"
	"sync"
	"time"
)

type peepTx struct {
	tx             *config.Transaction
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

func Peep(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running Peep...")
	time.Sleep(1 * time.Second)
	for i := 0; i < config.BlockSum; i++ { // 区块
		txs := make([]*config.Transaction, 0)
		for j := 0; j < config.TxSum/2; j++ { // 每个区块中的交易个数
			txs = append(txs, mp.GetIOTx())
			txs = append(txs, mp.GetCompetingTx())
		}

		start := time.Now()
		stateAccessSequence, inactivePeepTxs := constructStateAccessSequence(txs)
		for {
			newInactivePeepTxs := make([]*peepTx, 0)
			activePeepTxs := make([]*peepTx, 0)
			activePeepTxsChan := make(chan *config.Transaction, config.ChanLen)
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
				go s.Run(stateCache, activePeepTxsChan, wg, k)
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
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, config.TxSum/time.Since(start).Seconds())
	}
}

func constructStateAccessSequence(txs []*config.Transaction) (stateAccessSequence map[string]*addressAccessSequence, peepTxs []*peepTx) {
	stateAccessSequence = make(map[string]*addressAccessSequence)
	peepTxs = make([]*peepTx, 0)
	for i, tx := range txs { // 先统计出读写集
		writeKeys := make(map[string]struct{})
		readKeys := make(map[string]struct{})
		for _, key := range tx.Updates {
			if _, ok := writeKeys[key.Key]; !ok {
				writeKeys[key.Key] = struct{}{}
			}
		}

		for _, key := range tx.ReadKey {
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
