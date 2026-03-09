package mempool

import (
	"Janus/config"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// Mempool 表示交易池
type Mempool struct {
	// 交易的key都是数字，读取的时候要转成address
	ComputeTxs []*config.Transaction
	IOTxs      []*config.Transaction
	AllTxs     []*config.Transaction
}

// NewMempool 创建一个空交易池
func NewMempool() *Mempool {
	competingTxs, ioTxs := generateTxs()
	return &Mempool{
		ComputeTxs: competingTxs,
		IOTxs:      ioTxs,
		AllTxs:     make([]*config.Transaction, 0),
	}
}

func (m *Mempool) GetTx() *config.Transaction {
	rand.Seed(time.Now().UnixNano())
	txType := rand.Intn(2) + 1 // 1-2
	if txType == 1 {
		tx := m.ComputeTxs[0]
		m.ComputeTxs = m.ComputeTxs[1:]
		return tx
	} else {
		tx := m.IOTxs[0]
		m.IOTxs = m.IOTxs[1:]
		return tx
	}
}

func (m *Mempool) GetCompetingTx() *config.Transaction {
	tx := m.ComputeTxs[0]
	m.ComputeTxs = m.ComputeTxs[1:]
	return tx
}

func (m *Mempool) GetIOTx() *config.Transaction {
	tx := m.IOTxs[0]
	m.IOTxs = m.IOTxs[1:]
	return tx
}

func genTxs(txType config.TransactionType, txNum, writeN, readN, idx int, keyList []string) (int, []*config.Transaction) {
	transactions := make([]*config.Transaction, 0)
	for i := 0; i < txNum; i++ {

		wKeys := keyList[idx : idx+writeN]
		idx += writeN
		rKeys := keyList[idx : idx+readN]
		idx += readN

		updates := make([]config.KV, 0, writeN)
		for _, k := range wKeys {
			updates = append(updates, config.KV{Key: k, Value: []byte("value")})
		}
		//for _, k := range rKeys {
		//	updates = append(updates, config.KV{Key: k, Value: []byte("value")})
		//}

		reads := make([]string, 0)
		for _, k := range rKeys {
			reads = append(reads, k)
		}

		var txId string
		if txType == config.LongTx {
			txId = fmt.Sprintf("compute-%d", i)
		} else {
			txId = fmt.Sprintf("io-%d", i)
		}
		tx := &config.Transaction{
			ID:      txId,
			Type:    txType,
			Updates: updates,
			ReadKey: reads,
		}
		transactions = append(transactions, tx)
	}
	return idx, transactions
}

func generateTxs() ([]*config.Transaction, []*config.Transaction) {

	//// 创建一个迭代器
	//iter := key2AddrDB.NewIterator(nil, nil)
	//defer iter.Release() // 用完记得释放资源
	//
	//// 遍历整个数据库
	//for iter.Next() {
	//	key := iter.Key()
	//	value := iter.Value()
	//
	//	fmt.Printf("Key: %s, Value: %s\n", key, value)
	//}
	//
	//// 检查是否发生错误
	//if err := iter.Error(); err != nil {
	//	log.Fatal(err)
	//}
	//"4336668687"
	//addr, err := key2AddrDB.Get([]byte("4336668687"), nil)
	//if err != nil {
	//	fmt.Println("Get address error!!!,", err)
	//}
	//addrStr := common.BytesToAddress(addr).Hex()
	//fmt.Println("address:", addrStr, "key:", "4336668687")
	//return nil, nil

	rand.Seed(time.Now().UnixNano())

	totalNeeded := config.CompetingTxCount*config.CalcKeysPerTx + config.IoTxCount*config.IoKeysPerTx
	fmt.Println("需要生成唯一 key 的数量:", totalNeeded)

	// 生成全局唯一 key
	keys := make(map[int64]struct{}, totalNeeded)
	for len(keys) < totalNeeded {
		k := rand.Int63n(config.TotalKeys) + 1
		keys[k] = struct{}{}
	}

	// 转换为切片
	keyList := make([]string, 0, totalNeeded)
	for k := range keys {
		keyList = append(keyList, strconv.Itoa(int(k)))
	}
	// ---------------------
	// 2. 分配交易
	// ---------------------
	idx := 0

	// 生成计算型交易

	idx, competingTxs := genTxs(config.LongTx, config.CompetingTxCount, config.ComputingWriteN, config.ComputingReadN, idx, keyList)

	// 生成IO型交易
	idx, ioTxs := genTxs(config.ShortTx, config.IoTxCount, config.IoWriteN, config.IoReadN, idx, keyList)

	//for _, tx := range competingTxs {
	//	fmt.Println(tx.ReadKey)
	//}

	return competingTxs, ioTxs
}
