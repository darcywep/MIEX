package tools

import (
	janusConfig "Janus/config"
	"log"
	"math/big"
	"math/rand"
	"time"

	"Janus/ethereum/core/types"

	"github.com/ethereum/go-ethereum/common"
)

const contractBasePath = "/root/Janus/contract_example/"

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
func GenerateSmallBankTxs(addresses []common.Address, ioTxCount, cpuTxCount, fibonacciN int, skew float64) []*types.Transaction {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, skew, 1, uint64(len(addresses)-1))
	txCount := ioTxCount + cpuTxCount
	txs := make([]*types.Transaction, 0, txCount)
	gasPrice := big.NewInt(1)

	abiObject, _, err := LoadContract(contractBasePath+"smallbank_fibonacci.abi", contractBasePath+"smallbank_fibonacci.bin")
	PanicError(err)

	ioTxNum, cpuTxNum := 0, 0
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子（否则每次运行结果一样）

	for i := 1; i <= txCount; i++ {
		fromIdx := int(zipf.Uint64())
		toIdx := int(zipf.Uint64())
		if fromIdx == toIdx {
			toIdx = (toIdx + 1) % len(addresses)
		}

		from := addresses[fromIdx]
		to := addresses[toIdx]
		var (
			inputs []byte
			tx     *types.Transaction
		)
		// 随机生成一个CPU型交易或者是IO型交易
		txType := rand.Intn(2) + 1                                   // 生成 [1, 2] 之间的随机数
		if janusConfig.TransactionType(txType) == janusConfig.IOTx { // IO型交易
			if ioTxNum < ioTxCount {
				inputs, err = abiObject.Pack("transfer", to, big.NewInt(0).SetUint64(10))
				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), uint64(210000), gasPrice, inputs)
				tx.TxType = janusConfig.IOTx
				ioTxNum += 1
			} else {
				inputs, err = abiObject.Pack("fibonacciCalculate", to, big.NewInt(0).SetUint64(uint64(fibonacciN)))
				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), uint64(210000), gasPrice, inputs)
				tx.TxType = janusConfig.ComputeTx
				cpuTxNum += 1
			}
		} else { // CPU型交易
			if cpuTxNum < cpuTxCount {
				inputs, err = abiObject.Pack("fibonacciCalculate", to, big.NewInt(0).SetUint64(uint64(fibonacciN)))
				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), uint64(210000), gasPrice, inputs)
				tx.TxType = janusConfig.ComputeTx
				cpuTxNum += 1
			} else {
				inputs, err = abiObject.Pack("transfer", to, big.NewInt(0).SetUint64(10))
				tx = types.NewTransaction(uint64(0), ContractAddress, big.NewInt(0), uint64(210000), gasPrice, inputs)
				tx.TxType = janusConfig.IOTx
				ioTxNum += 1
			}
		}

		PanicError(err)
		// 注意交易的调用地址要用之前的合约地址
		tx.SmallBankTo = to
		tx.SetFrom(from)
		txs = append(txs, tx)
	}
	log.Printf("Zipf 生成交易完成（skew=%.2f）", skew)
	return txs
}
