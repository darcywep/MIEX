package init_smallbank_contract

import (
	"fmt"
	levm "janus-geth-1165/core/evm"
	"janus-geth-1165/ethereum/core/tracing"
	"janus-geth-1165/ethereum/database"
	"janus-geth-1165/tools"
	"math/big"
	"os"
	"path"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

const totalAccounts = 10_000_000 // 2000W

var (
	basePath = "/root/janus-geth-1165/contract_example/"
	abiFile  = path.Join(basePath, "smallbank_m_fibonacci.abi")
	binFile  = path.Join(basePath, "smallbank_m_fibonacci.bin")

	// ========= 初始化状态数据库 =========
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   65536, // 64GB
		Handles: 32786,
	}
)

func loadContractInfo() (abi.ABI, []byte) {
	abiObject, binData, err := tools.LoadContract(abiFile, binFile)
	tools.PanicError("loadContractInfo ", err)
	return abiObject, binData
}

func intToAddress(i int) common.Address {
	var addrInt = new(big.Int).SetUint64(uint64(i))
	return common.BytesToAddress(addrInt.Bytes())
}

func TestSmallBank() {
	// ========= 载入合约 =========
	abiObject, binData := loadContractInfo()
	// 创建系统账户（合约部署者）
	fromAddr := tools.GenerateAddress()
	evm := levm.New(stateConfig, big.NewInt(0), common.Hash{}, fromAddr)
	//defer evm.AllDB().Close()
	balance := new(uint256.Int).SetUint64(1e18)
	balance.Mul(balance, new(uint256.Int).SetUint64(1e6)) // 100万ETH
	evm.NewAccount(fromAddr, balance)                     // 给足够多的ETH

	// ========= 部署合约 =========
	_, cAddress, _, err := evm.DeployContract(fromAddr, binData)
	tools.PanicError("TestSmallBank DeployContract", err)
	fmt.Println("✅ SmallBank deployed at:", cAddress.Hex())

	// ========= 参数设置 =========

	depositAmount := new(uint256.Int).SetUint64(10e18)
	//var parentRoot common.Hash = common.Hash{} // genesis block

	// ========= 主循环 =========
	for i := 1; i <= totalAccounts; i++ {
		user := intToAddress(i)
		evm.NewAccount(user, depositAmount)
		//fmt.Println(user)
		//tools.CatStorageState = false
		_, err = evm.CallContractABI(user, cAddress, new(uint256.Int).SetUint64(0), abiObject,
			"deposit", big.NewInt(0).SetUint64(10e18))

		//tools.CatStorageState = true
		//_, err = evm.CallContractABI(user, cAddress, new(uint256.Int).SetUint64(0), abiObject,
		//	"getBalance", user)

		if err != nil {
			fmt.Printf("Account #%d deposit error: %v\n", i, err)
		}

		// 每10万次打印进度
		if i%100000 == 0 {
			fmt.Printf("Progress: %d / %d\n", i, totalAccounts)
		}
		//if i%10000000 == 0 {
		//	// ========= 中间状态根输出 =========
		//	stateRoot, _, err := evm.AllDB().StateDB.CommitWithUpdate(uint64(0), true, true)
		//	tools.PanicError("StateDB.CommitWithUpdate ", err)
		//	err = evm.AllDB().StateDB.Database().TrieDB().Commit(stateRoot, false)
		//	tools.PanicError("StateDB.Database().TrieDB().Commit", err)
		//	fmt.Println("🌳 Intermediate state root at account", i, ":", stateRoot.Hex())
		//	err = evm.AllDB().UpdateStateDB(stateRoot)
		//	tools.PanicError("evm.AllDB().UpdateStateDB", err)
		//
		//	err = evm.AllDB().StateDB.Database().TrieDB().Dereference(parentRoot)
		//	tools.PanicError("evm.AllDB().StateDB.Database().TrieDB().Dereference", err)
		//
		//	err = evm.AllDB().StateDB.Database().TrieDB().Cap(0)
		//	tools.PanicError("TestSmallBank evm.AllDB().StateDB.Database().TrieDB().Cap ", err)
		//
		//	evm.NewAllDB(stateConfig, big.NewInt(0), stateRoot)
		//	parentRoot = stateRoot
		//}
	}

	// ========= 导出状态根 =========
	stateRoot, _, err := evm.AllDB().StateDB.CommitWithUpdate(uint64(0), true, true)
	tools.PanicError("TestSmallBank Finish all, evm.AllDB().StateDB.CommitWithUpdate", err)
	err = evm.AllDB().StateDB.Database().TrieDB().Commit(stateRoot, false)
	tools.PanicError("TestSmallBank Finish all, evm.AllDB().StateDB.Database().TrieDB().Commit", err)
	fmt.Println("🌳 Final state root:", stateRoot.Hex())

	// ========= 输出到txt =========
	resultFile := path.Join("/root/alldb/", "smallbank_result.txt")
	content := fmt.Sprintf("Contract Address: %s\nFinal State Root: %s\n", cAddress.Hex(), stateRoot.Hex())
	err = os.WriteFile(resultFile, []byte(content), 0644)
	tools.PanicError("TestSmallBank Finish all, os.WriteFile", err)
	fmt.Println("✅ Done! Result saved to:", resultFile)
	evm.AllDB().Close()
}

func TestSmallBankWithExistDB() {
	// ========= 载入合约 =========
	abiObject, _ := loadContractInfo()

	evm := levm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer evm.AllDB().Close()

	for i := 1; i <= totalAccounts; i++ {
		user := intToAddress(i)
		//fmt.Println(evm.AllDB().StateDB.GetBalance(user))
		tools.CatStorageState = false
		_, err := evm.CallContractABI(user, tools.ContractAddress, new(uint256.Int).SetUint64(0), abiObject,
			"getBalance", user)
		if err != nil {
			fmt.Printf("Account #%d getBalance error: %v\n", i, err)
		}
		fmt.Println()
		tools.CatStorageState = true
		//to := intToAddress(i + 10)
		//_, err = evm.CallContractABI(user, tools.ContractAddress, new(uint256.Int).SetUint64(0), abiObject,
		//	"transfer", to,
		//	big.NewInt(0).SetUint64(10),
		//	big.NewInt(0).SetUint64(10),
		//	big.NewInt(0).SetUint64(1000000),
		//	false)
		_, err = evm.CallContractABI(user, tools.ContractAddress, new(uint256.Int).SetUint64(0), abiObject,
			"fibonacciCalculate", user,
			big.NewInt(0).SetUint64(10),
			big.NewInt(0).SetUint64(10000),
			true)
		fmt.Println("\n")
		if i >= 10 {
			return
		}
	}
}

func TestSmallBankTransferWithExistDB() {
	// ========= 载入合约 =========
	abiObject, _ := loadContractInfo()

	evm := levm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer evm.AllDB().Close()
	tools.CatStorageState = true
	for i := 1; i <= totalAccounts; i++ {
		user := intToAddress(i)
		fmt.Println(user)
		_, err := evm.CallContractABI(user, tools.ContractAddress, new(uint256.Int).SetUint64(0), abiObject,
			"getBalance", user)
		fmt.Println()
		if err != nil {
			fmt.Printf("Account #%d balance error: %v\n", i, err)
		}
		if i >= 10 {
			return
		}
	}
}

func ChangeContractCode() {
	// ========= 载入合约 =========
	_, abiBin := loadContractInfo()

	evm := levm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer evm.AllDB().Close()

	evm.AllDB().StateDB.SetCode(tools.ContractAddress, abiBin, tracing.CodeChangeContractCreation)

	// ========= 导出状态根 =========
	stateRoot, _, err := evm.AllDB().StateDB.CommitWithUpdate(uint64(0), true, true)
	tools.PanicError("ChangeContractCode evm.AllDB().StateDB.CommitWithUpdate", err)
	err = evm.AllDB().StateDB.Database().TrieDB().Commit(stateRoot, false)
	tools.PanicError("ChangeContractCode evm.AllDB().StateDB.Database().TrieDB().Commit ", err)
	fmt.Println("🌳 Final state root:", stateRoot.Hex())
}

// 28e0e1a20000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000001570000000000000000000000000000000000000000000000000000000000000000
// 28e0e1a20000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000027100000000000000000000000000000000000000000000000000000000000000001
