package init_smallbank_contract

import (
	levm "Janus/core/evm"
	"Janus/ethereum/core/tracing"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"math/big"
	"os"
	"path"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

const totalAccounts = 100_000_000 // 1亿

var (
	basePath = "/root/Janus/contract_example/"
	abiFile  = path.Join(basePath, "smallbank_fibonacci.abi")
	binFile  = path.Join(basePath, "smallbank_fibonacci.bin")

	// ========= 初始化状态数据库 =========
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   65536, // 64GB
		Handles: 32786,
	}
)

func loadContractInfo() (abi.ABI, []byte) {
	abiObject, binData, err := tools.LoadContract(abiFile, binFile)
	tools.PanicError(err)
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
	defer evm.AllDB().Close()
	evm.NewAccount(fromAddr, new(uint256.Int).SetUint64(2e18)) // 给足够多的ETH

	// ========= 部署合约 =========
	_, cAddress, _, err := evm.DeployContract(fromAddr, binData)
	tools.PanicError(err)
	fmt.Println("✅ SmallBank deployed at:", cAddress.Hex())

	// ========= 参数设置 =========

	depositAmount := new(uint256.Int).SetUint64(10e18)

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
	}

	// ========= 导出状态根 =========
	stateRoot, _, err := evm.AllDB().StateDB.CommitWithUpdate(uint64(0), true, true)
	tools.PanicError(err)
	err = evm.AllDB().StateDB.Database().TrieDB().Commit(stateRoot, false)
	tools.PanicError(err)
	fmt.Println("🌳 Final state root:", stateRoot.Hex())

	// ========= 输出到txt =========
	resultFile := path.Join("/root/alldb/", "smallbank_result.txt")
	content := fmt.Sprintf("Contract Address: %s\nFinal State Root: %s\n", cAddress.Hex(), stateRoot.Hex())
	err = os.WriteFile(resultFile, []byte(content), 0644)
	tools.PanicError(err)
	fmt.Println("✅ Done! Result saved to:", resultFile)
}

func TestSmallBankWithExistDB() {
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
	tools.PanicError(err)
	err = evm.AllDB().StateDB.Database().TrieDB().Commit(stateRoot, false)
	tools.PanicError(err)
	fmt.Println("🌳 Final state root:", stateRoot.Hex())
}
