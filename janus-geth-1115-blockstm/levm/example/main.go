package main

import (
	"chukonu/database"
	"chukonu/levm"
	"chukonu/tools"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

func testExample() {
	//make a new address evm
	fromAddr := tools.GenerateAddress()

	basePath := "/Users/darcywep/GolandProjects/Janus_blockstm/levm/example/contract/"

	//Load a contract from file
	abiObject, binData, err := tools.LoadContract(basePath+"fibonacci.abi", basePath+"fibonacci.bin")
	fmt.Println("Abi\n", abiObject.Methods)

	stateConfig := &database.StateDBConfig1165{
		Path:          "./db",
		Cache:         4096,
		Handles:       4096,
		TrieCache:     4096,
		TriePreimages: false,
	}

	// create new levm instance
	lvm := levm.New(stateConfig, big.NewInt(0), common.Hash{}, fromAddr)

	// create a new account and set the balance
	// (needs enough balance to cover gas cost)
	lvm.NewAccount(fromAddr, new(big.Int).SetUint64(2e18))

	// deploy a contract
	code, addr, gas, err := lvm.DeployContract(fromAddr, binData)
	fmt.Println("contract code length:", len(code))
	fmt.Printf("contract address: %x\n", addr)
	fmt.Println("unused gas:", gas)
	fmt.Println("errors:", err)
}

// TestSimulation test concurrent transaction simulations
func testFibonacci() {
	var evm *levm.LEVM
	//var fromAddress []common.Address
	var cAddress common.Address
	basePath := "/Users/darcywep/GolandProjects/Janus_blockstm/levm/example/contract/"
	abiObject, binData, err := tools.LoadContract(basePath+"fibonacci.abi", basePath+"fibonacci.bin")
	if err != nil {
		fmt.Println(err)
	}

	stateConfig := &database.StateDBConfig1165{
		Path:          "./db",
		Cache:         4096,
		Handles:       4096,
		TrieCache:     4096,
		TriePreimages: false,
	}
	fromAddr := tools.GenerateAddress()
	evm = levm.New(stateConfig, big.NewInt(0), common.Hash{}, fromAddr)
	evm.NewAccount(fromAddr, new(big.Int).SetUint64(2e18))
	_, addr, _, err := evm.DeployContract(fromAddr, binData)
	tools.PanicError("DeployContract", err)

	cAddress = addr
	_, err = evm.CallContractABI(fromAddr, cAddress, new(big.Int).SetUint64(0),
		abiObject, "calculate", big.NewInt(10))
	tools.PanicError("CallContractABI", err)

	tools.CatStorageState = true
	_, err = evm.CallContractABI(fromAddr, cAddress, new(big.Int).SetUint64(0),
		abiObject, "getUserLastResult", fromAddr)
	tools.PanicError("CallContractABI", err)

}

func main() {
	//testExample()
	testFibonacci()
}
