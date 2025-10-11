package mempool

import (
	"MixedLoadTransactionConcurrency/config"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	competingTxCount = 1000000 // 计算型交易数
	ioTxCount        = 1000000 // IO 型交易数
	calcKeysPerTx    = 4       // 每个计算交易读的 key 数
	ioKeysPerTx      = 105     // 每个 IO 交易读的 key 数
)

// Mempool 表示交易池
type Mempool struct {
	ComputeTxs []*config.Transaction
	IOTxs      []*config.Transaction
	AllTxs     []*config.Transaction
}

// NewMempool 创建一个空交易池
func NewMempool(key2AddrDBPath string) *Mempool {
	competingTxs, ioTxs := generateTxs(key2AddrDBPath)
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
		reads := make([]string, 0)
		for _, k := range rKeys {
			updates = append(updates, config.KV{Key: k, Value: []byte("value")})
		}

		var txId string
		if txType == config.ComputeTx {
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

func generateTxs(key2AddrDBPath string) ([]*config.Transaction, []*config.Transaction) {
	key2AddrDB, err := leveldb.OpenFile(key2AddrDBPath, &opt.Options{
		BlockCacheCapacity: 0, // 禁用 block cache
		WriteBuffer:        0, // 禁用写缓冲
		Strict:             opt.DefaultStrict,
	})
	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	defer key2AddrDB.Close()
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
	addr, err := key2AddrDB.Get([]byte("4336668687"), nil)
	if err != nil {
		fmt.Println("Get address error!!!,", err)
	}
	addrStr := common.BytesToAddress(addr).Hex()
	fmt.Println("address:", addrStr, "key:", "4336668687")
	return nil, nil

	rand.Seed(time.Now().UnixNano())

	totalNeeded := competingTxCount*calcKeysPerTx + ioTxCount*ioKeysPerTx
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
		addr, err := key2AddrDB.Get([]byte(strconv.Itoa(int(k))), nil)
		if err != nil {
			fmt.Println("Get address error!!!,", err)
		}
		addrStr := common.BytesToAddress(addr).Hex()
		fmt.Println("address:", addrStr, "key:", k)

		keyList = append(keyList, addrStr)
	}

	// ---------------------
	// 2. 分配交易
	// ---------------------
	idx := 0

	// 生成计算型交易
	writeN := rand.Intn(2) + 1 // 1-2
	readN := rand.Intn(2) + 1  // 1-2

	idx, competingTxs := genTxs(config.ComputeTx, competingTxCount, writeN, readN, idx, keyList)

	// 生成IO型交易
	writeN = rand.Intn(1) + 0  // 6-10
	readN = rand.Intn(3) + 100 // 8-10
	idx, ioTxs := genTxs(config.IOTx, ioTxCount, writeN, readN, idx, keyList)

	return competingTxs, ioTxs
}
