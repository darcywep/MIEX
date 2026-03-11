package tools

import (
	janusConfig "Janus/config"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	"github.com/chinuy/zipf"

	"Janus/ethereum/core/types"

	"github.com/ethereum/go-ethereum/common"
)

const ContractBasePath = "/root/Janus/contract_example/"

// GenerateAddresses 生成指定范围的伪地址
func GenerateAddresses(start, end int) []common.Address {
	addresses := make([]common.Address, 0, end-start+1)
	for i := start; i <= end; i++ {
		addr := common.BigToAddress(big.NewInt(int64(i)))
		addresses = append(addresses, addr)
	}
	return addresses
}

// GenerateSmallBankTxs 基于Zipf分布生成交易，用于控制冲突概率
//func GenerateSmallBankTxs(addresses []common.Address, ioTxCount, cpuTxCount, fibonacciN int, recursive bool, skew float64) []*types.Transaction {
//	r := rand.New(rand.NewSource(time.Now().UnixNano()))
//	z := zipf.NewZipf(r, skew, uint64(len(addresses)-1))
//
//	txCount := ioTxCount + cpuTxCount
//	txs := make([]*types.Transaction, 0, txCount)
//	gasPrice := big.NewInt(10)
//
//	abiObject, _, err := LoadContract(ContractBasePath+"smallbank_m_fibonacci.abi", ContractBasePath+"smallbank_m_fibonacci.bin")
//	PanicError("GenerateSmallBankTxs LoadContract ", err)
//
//	ioTxNum, cpuTxNum := 0, 0
//	rand.Seed(time.Now().UnixNano()) // 设置随机数种子（否则每次运行结果一样）
//
//	for i := 1; i <= txCount; i++ {
//		fromIdx := int(z.Uint64())
//		toIdx := int(z.Uint64())
//
//		if fromIdx == toIdx {
//			toIdx = (toIdx + 1) % len(addresses)
//		}
//
//		from := addresses[fromIdx]
//		to := addresses[toIdx]
//		//fmt.Println("fromIdx:", fromIdx, " toIdx:", toIdx)
//		//fmt.Println("from:", from, " to:", to)
//		var (
//			inputs []byte
//			tx     *types.Transaction
//		)
//		// 随机生成一个CPU型交易或者是IO型交易
//		txType := rand.Intn(2) + 1 // 生成 [1, 2] 之间的随机数
//		//ioFibonacciM, cpuFibonacciM := rand.Intn(5)+5, rand.Intn(10)+10
//		ioFibonacciM, cpuFibonacciM := 20, 40
//		if janusConfig.TransactionType(txType) == janusConfig.ShortTx { // IO型交易
//			if ioTxNum < ioTxCount {
//				inputs, err = abiObject.Pack(
//					"transfer",
//					to,
//					big.NewInt(0).SetUint64(10),
//					big.NewInt(0).SetUint64(uint64(fibonacciN)),
//					big.NewInt(0).SetUint64(uint64(ioFibonacciM)),
//					recursive,
//				)
//				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
//				tx.TxType = janusConfig.ShortTx
//				ioTxNum += 1
//			} else {
//				inputs, err = abiObject.Pack(
//					"fibonacciCalculate",
//					to,
//					big.NewInt(0).SetUint64(uint64(fibonacciN)),
//					big.NewInt(0).SetUint64(uint64(cpuFibonacciM)),
//					recursive,
//				)
//				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
//				tx.TxType = janusConfig.LongTx
//				cpuTxNum += 1
//			}
//		} else { // CPU型交易
//			if cpuTxNum < cpuTxCount {
//				inputs, err = abiObject.Pack(
//					"fibonacciCalculate",
//					to,
//					big.NewInt(0).SetUint64(uint64(fibonacciN)),
//					big.NewInt(0).SetUint64(uint64(cpuFibonacciM)),
//					recursive,
//				)
//				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
//				tx.TxType = janusConfig.LongTx
//				cpuTxNum += 1
//			} else {
//				inputs, err = abiObject.Pack(
//					"transfer",
//					to,
//					big.NewInt(0).SetUint64(10),
//					big.NewInt(0).SetUint64(uint64(fibonacciN)),
//					big.NewInt(0).SetUint64(uint64(ioFibonacciM)),
//					recursive,
//				)
//				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
//				tx.TxType = janusConfig.ShortTx
//				ioTxNum += 1
//			}
//		}
//		PanicError("GenerateSmallBankTxs abiObject.Pack ", err)
//		// 注意交易的调用地址要用之前的合约地址
//		tx.SmallBankTo = to
//		tx.SetFrom(from)
//		txs = append(txs, tx)
//	}
//	fmt.Printf("Zipf 生成交易完成（skew=%.2f）", skew)
//	return txs
//}

// GenerateSmallBankTxs 基于Zipf分布生成交易，用于控制冲突概率
func GenerateSmallBankTxs(addresses []common.Address, shortTxCount, longTxCount, fibonacciN int, recursive bool, skew float64) []*types.Transaction {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := zipf.NewZipf(r, skew, uint64(len(addresses)-1))

	txCount := shortTxCount + longTxCount
	txs := make([]*types.Transaction, 0, txCount)
	gasPrice := big.NewInt(10)

	abiObject, _, err := LoadContract(ContractBasePath+"smallbank_m_fibonacci.abi", ContractBasePath+"smallbank_m_fibonacci.bin")
	PanicError("GenerateSmallBankTxs LoadContract ", err)

	longTxNum, shortTxNum := 0, 0
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子（否则每次运行结果一样）

	for i := 1; i <= txCount; i++ {
		fromIdx := int(z.Uint64())
		toIdx := int(z.Uint64())

		for fromIdx == toIdx {
			toIdx = int(z.Uint64())
		}

		from := addresses[fromIdx+1]
		to := addresses[toIdx+1]
		var (
			inputs              []byte
			tx                  *types.Transaction
			fibonacciLoopNumber int
		)
		// 随机生成一个CPU型交易或者是IO型交易
		txType := rand.Intn(2) + 1 // 生成 [1, 2] 之间的随机数
		isLongTx := false
		if txType == 1 { // 长交易
			if longTxNum < longTxCount {
				isLongTx = true
				longTxNum++
			} else {
				shortTxNum++
			}
		} else { // 短交易
			if shortTxNum < shortTxCount {
				shortTxNum++
			} else {
				isLongTx = true
				longTxNum++
			}
		}

		if isLongTx {
			txType = 1
			fibonacciLoopNumber = 40
		} else {
			txType = 2
			fibonacciLoopNumber = 20
		}
		inputs, err = abiObject.Pack(
			"transfer",
			to,
			big.NewInt(0).SetUint64(10),
			big.NewInt(0).SetUint64(uint64(fibonacciN)),
			big.NewInt(0).SetUint64(uint64(fibonacciLoopNumber)),
			recursive,
		)
		tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
		tx.TxType = janusConfig.TransactionType(txType)
		PanicError("GenerateSmallBankTxs abiObject.Pack ", err)
		// 注意交易的调用地址要用之前的合约地址
		tx.SmallBankTo = to
		tx.SetFrom(from)
		txs = append(txs, tx)
	}
	fmt.Printf("Zipf 生成交易完成（skew=%.2f）", skew)
	return txs
}

func GenerateTxsFormBriefTx(btxs [][]int, recursive bool) []*types.Transaction {
	txs := make([]*types.Transaction, 0, len(btxs))
	abiObject, _, err := LoadContract(ContractBasePath+"smallbank_m_fibonacci.abi", ContractBasePath+"smallbank_m_fibonacci.bin")
	PanicError("GenerateSmallBankTxs LoadContract ", err)
	gasPrice := big.NewInt(10)
	for _, btx := range btxs {
		//fmt.Println(btx[0], btx[1], btx[2], btx[3], btx[4])
		//btx[3] = 0
		from, to := common.BigToAddress(big.NewInt(int64(btx[0]))), common.BigToAddress(big.NewInt(int64(btx[1])))
		inputs, err := abiObject.Pack(
			"transfer",
			to,
			big.NewInt(0).SetUint64(10),
			big.NewInt(0).SetUint64(uint64(btx[3])),
			big.NewInt(0).SetUint64(uint64(btx[4])),
			recursive,
		)
		tx := types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), Uint64, gasPrice, inputs)
		tx.TxType = janusConfig.TransactionType(btx[2])
		PanicError("GenerateSmallBankTxs abiObject.Pack ", err)
		// 注意交易的调用地址要用之前的合约地址
		tx.SmallBankTo = to
		tx.SetFrom(from)
		txs = append(txs, tx)
	}

	return txs
}

// GenerateBaseTransaction 基于Zipf分布生成交易，用于控制冲突概率
func GenerateBaseTransaction(addressLen int, longTxCount, shortTxCount, fibonacciN, shortTxFibonacciLoopNumber, longTxFibonacciLoopNumber int, skew float64) [][]int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := zipf.NewZipf(r, skew, uint64(addressLen+1))

	txCount := longTxCount + shortTxCount
	txs := make([][]int, 0, txCount)

	longTxNum, shortTxNum := 0, 0
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子（否则每次运行结果一样）

	for i := 1; i <= txCount; i++ {
		from := int(z.Uint64())
		to := int(z.Uint64())

		for from == to {
			to = int(z.Uint64())
		}
		// tx []int = [from, to, txType, fibonacciN]
		tx := make([]int, 5)
		tx[0] = from + 1
		tx[1] = to + 1

		// 随机生成一个长交易或者是短交易
		tx[2] = rand.Intn(2) + 1 // 生成 [1, 2] 之间的随机数

		isLongTx := false
		if tx[2] == 1 { // 长交易
			if longTxNum < longTxCount {
				isLongTx = true
				longTxNum++
			} else {
				shortTxNum++
			}
		} else { // 短交易
			if shortTxNum < shortTxCount {
				shortTxNum++
			} else {
				isLongTx = true
				longTxNum++
			}
		}

		if isLongTx {
			tx[2] = 1
		} else {
			tx[2] = 2
		}

		if fibonacciN == -1 {
			if isLongTx {
				tx[3] = rand.Intn(5) + 31 // 31-35
				tx[4] = longTxFibonacciLoopNumber
			} else {
				tx[3] = rand.Intn(5) + 1 // 1-5
				tx[4] = shortTxFibonacciLoopNumber
			}
		} else {
			tx[3] = fibonacciN
			if isLongTx {
				tx[4] = longTxFibonacciLoopNumber
			} else {
				tx[4] = shortTxFibonacciLoopNumber
			}
		}
		//tx[4] = fibonacciLoopNumber
		txs = append(txs, tx)
	}
	return txs
}
