package main

import (
	levm "Janus/core/evm"
	"Janus/core/evm/example/init_smallbank_contract"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func testExample() {
	//make a new address evm
	fromAddr := tools.GenerateAddress()

	//Load a contract from file
	abiObject, binData, err := tools.LoadContract("contract/example_sol_Example.abi", "contract/example_sol_Example.bin")
	fmt.Println("Abi\n", abiObject.Methods)

	stateConfig := &database.StateDBConfig{
		Path:    "./db",
		Cache:   4096,
		Handles: 4096,
	}

	// create new levm instance
	lvm := levm.New(stateConfig, big.NewInt(0), common.Hash{}, fromAddr)

	// create a new account and set the balance
	// (needs enough balance to cover gas cost)
	lvm.NewAccount(fromAddr, new(uint256.Int).SetUint64(2e18))

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
	path := "/Users/darcywep/GolandProjects/Janus/core/evm/example/contract/"
	abiObject, binData, err := tools.LoadContract(path+"fibonacci.abi", path+"fibonacci.bin")
	if err != nil {
		fmt.Println(err)
	}

	stateConfig := &database.StateDBConfig{
		Path:    path + "db",
		Cache:   4096,
		Handles: 4096,
	}
	fromAddr := tools.GenerateAddress()
	evm = levm.New(stateConfig, big.NewInt(0), common.Hash{}, fromAddr)
	evm.NewAccount(fromAddr, new(uint256.Int).SetUint64(2e18))
	_, addr, _, err := evm.DeployContract(fromAddr, binData)
	tools.PanicError("testFibonacci DeployContract ", err)

	cAddress = addr
	_, err = evm.CallContractABI(fromAddr, cAddress, new(uint256.Int).SetUint64(0),
		abiObject, "calculate", big.NewInt(20))
	tools.PanicError("testFibonacci CallContractABI calculate ", err)

	_, err = evm.CallContractABI(fromAddr, cAddress, new(uint256.Int).SetUint64(0),
		abiObject, "getUserLastResult", fromAddr)
	tools.PanicError("testFibonacci CallContractABI getUserLastResult ", err)

}

func main() {
	//testExample()
	//testFibonacci()
	//init_smallbank_contract.TestSmallBank()
	init_smallbank_contract.TestSmallBankWithExistDB()
	//init_smallbank_contract.ChangeContractCode()
}
