package generator

import (
	"Janus/baselines/common"
	"fmt"
	"strings"
	"sync/atomic"
)

// ============================ 类型定义 ============================

// TxGenerator 事务生成器
type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*common.Block
	idCounter int32
}

// NewTxGenerator 创建事务生成器
func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	if blockSize == 0 {
		blockSize = common.BLOCK_SIZE
	}

	return &TxGenerator{
		txNum:     txNum,
		blockSize: blockSize,
		blocks:    make([]*common.Block, 0),
		idCounter: 0,
	}
}

// GenerateWorkload 生成负载
func (tg *TxGenerator) GenerateWorkload(isNest bool) []*common.Block {
	workload := common.NewWorkload()

	seed := workload.GetSeed()

	fmt.Printf("block size: %d\n", tg.blockSize)
	fmt.Printf("seed: %d\n", seed)

	// 生成区块
	blockNum := tg.txNum / tg.blockSize
	fmt.Printf("block num: %d\n", blockNum)

	for i := 0; i < blockNum; i++ {
		block := tg.GenerateBlock(isNest, workload, i+1)
		tg.blocks = append(tg.blocks, block)
	}

	return tg.blocks
}

// GenerateBlock 生成区块
func (tg *TxGenerator) GenerateBlock(isNest bool, workload *common.Workload, blockID int) *common.Block {
	totalCost := 0
	//txLists := make([]*common.Vertex, 0)
	txs := make([]*common.Transaction, 0)
	txInfos := make([]*common.HyperVertex, 0)
	invertedIndex := make(map[string]*common.RWSets)
	RWIndex := make(map[*common.Vertex]map[*common.Vertex]bool)
	conflictIndex := make(map[*common.Vertex]map[*common.Vertex]bool)
	RBIndex := make(map[string]map[*common.Vertex]bool)

	// 生成事务
	for i := 0; i < tg.blockSize; i++ {

		fmt.Printf("i: %d\n", i)

		// 生成TPCC事务
		tx := workload.NextTransaction()

		// 构建事务
		txVertex := tg.GenerateTransaction(tx, isNest, invertedIndex)

		// 记录所有子事务
		//for vertex, _ := range txVertex.Vertices {
		//	txLists = append(txLists, vertex)
		//}
		// 记录所有事务
		//transaction := &common.Transaction{
		//	common.HyperVertex txVertex
		//}

		transaction := common.NewTransaction(
			txVertex,
		)

		txs = append(txs, transaction)
		txInfos = append(txInfos, txVertex)

		// 计算总成本
		totalCost += txVertex.RootVertex.Cost
	}

	// 生成索引
	//tg.GenerateIndex(txLists, invertedIndex, RWIndex, conflictIndex, RBIndex)

	// 生成并返回区块
	return common.NewBlock(blockID, txs, txInfos, invertedIndex, RWIndex, conflictIndex, RBIndex, totalCost)
}

// GenerateTransaction 生成事务
func (tg *TxGenerator) GenerateTransaction(tx *common.TPCCTransaction, isNest bool, invertedIndex map[string]*common.RWSets) *common.HyperVertex {
	// range of txid: [1, BLOCK_SIZE] => (x - 1) % BLOCK_SIZE + 1
	txid := (tg.GetID()-1)%tg.blockSize + 1
	hyperVertex := common.NewHyperVertex(txid, isNest)
	rootVertex := common.NewVertex(hyperVertex, txid, fmt.Sprintf("%d", txid), 0, isNest)
	txidStr := fmt.Sprintf("%d", txid)

	if isNest {
		hyperVertex.BuildVertexs(tx, hyperVertex, rootVertex, txidStr, invertedIndex)
		// 记录超节点包含的所有节点
		for vertex := range rootVertex.CascadeVertices {
			hyperVertex.Vertices[vertex] = true
		}
		// 根据子节点依赖更新回滚代价和级联子事务
		hyperVertex.RootVertex = rootVertex
		hyperVertex.RecognizeCascades(rootVertex)
	} else {
		//hyperVertex.BuildVertexsSimple(tx, rootVertex, invertedIndex)
		hyperVertex.BuildVertexsSimple(tx, rootVertex, invertedIndex)
		// 添加回滚代价
		rootVertex.Cost = rootVertex.SelfCost
		// 更新读写集
		rootVertex.AllReadSet = rootVertex.ReadSet
		rootVertex.AllWriteSet = rootVertex.WriteSet
		// 添加自己
		rootVertex.CascadeVertices[rootVertex] = true
		hyperVertex.Vertices[rootVertex] = true
		hyperVertex.RootVertex = rootVertex
	}

	return hyperVertex
}

// GenerateIndex 生成事务索引
func (tg *TxGenerator) GenerateIndex(
	txLists []*common.Vertex,
	invertedIndex map[string]*common.RWSets,
	RWIndex map[*common.Vertex]map[*common.Vertex]bool,
	conflictIndex map[*common.Vertex]map[*common.Vertex]bool,
	RBIndex map[string]map[*common.Vertex]bool,
) {
	// 利用invertedIndex构建RWIndex
	for key, rwSets := range invertedIndex {
		readTxs := rwSets.ReadSet
		writeTxs := rwSets.WriteSet

		// 判断是否有写事务，若没有写事务则跳过
		if len(writeTxs) == 0 {
			continue
		}

		// 针对"Dytd-", "S-", "Cdelivery-"开头的key, 构建RBIndex
		if strings.HasPrefix(key, "Dytd-") || strings.HasPrefix(key, "S-") || strings.HasPrefix(key, "Cdelivery-") {
			// 找到在readTxs和writeTxs中都存在的事务
			intersectTxs := make(map[*common.Vertex]bool)
			for rTx := range readTxs {
				if writeTxs[rTx] {
					intersectTxs[rTx] = true
				}
			}

			if len(intersectTxs) <= 1 {
				continue
			}

			// 将这些事务加入RBIndex
			RBIndex[key] = intersectTxs
			continue
		}

		for rTx := range readTxs {
			if RWIndex[rTx] == nil {
				RWIndex[rTx] = make(map[*common.Vertex]bool)
			}

			for wTx := range writeTxs {
				if wTx != rTx {
					RWIndex[rTx][wTx] = true
				}
			}
		}
	}

	// 利用invertedIndex构建conflictIndex
	for _, tx := range txLists {
		// 初始化冲突索引
		if conflictIndex[tx] == nil {
			conflictIndex[tx] = make(map[*common.Vertex]bool)
		}

		// 遍历交易写集，获得并保存与写集冲突的所有交易
		for wKey := range tx.WriteSet {
			if rwSets, exists := invertedIndex[wKey]; exists {
				// 添加写集冲突
				for wTx := range rwSets.WriteSet {
					if wTx != tx {
						conflictIndex[tx][wTx] = true
					}
				}
				// 添加读集冲突
				for rTx := range rwSets.ReadSet {
					if rTx != tx {
						conflictIndex[tx][rTx] = true
					}
				}
			}
		}

		// 遍历交易读集，获得并保存与读集冲突的所有写交易
		for rKey := range tx.ReadSet {
			if rwSets, exists := invertedIndex[rKey]; exists {
				for wTx := range rwSets.WriteSet {
					if wTx != tx {
						conflictIndex[tx][wTx] = true
					}
				}
			}
		}
	}
}

// GetBlocks 获取区块
func (tg *TxGenerator) GetBlocks() []*common.Block {
	return tg.blocks
}

// GetID 获取全局事务ID
func (tg *TxGenerator) GetID() int {
	return int(atomic.AddInt32(&tg.idCounter, 1))
}
