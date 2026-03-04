package levm

import (
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/database"
	"chukonu/ethdb"
	"chukonu/tools"
	"fmt"
	"math/big"

	"chukonu/levm/vminterface"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/params"

	"chukonu/core/vm"

	"github.com/ethereum/go-ethereum/common"
)

// LEVM is a container for the go-ethereum EVM
// with methods to create and call contracts.
//
// LEVM contains the two most important objects
// for interacting with the EVM: stateDB and
// vm.EVM. The LEVM should be created with the
// LEVM.New() method, unless you know what you
// doing.

// create the evm
var NewChainConfig = &params.ChainConfig{
	ChainID:                 big.NewInt(1),
	HomesteadBlock:          big.NewInt(0),
	DAOForkBlock:            big.NewInt(0),
	DAOForkSupport:          true,
	EIP150Block:             big.NewInt(0),
	EIP155Block:             big.NewInt(0),
	EIP158Block:             big.NewInt(0),
	ByzantiumBlock:          big.NewInt(0),
	ConstantinopleBlock:     big.NewInt(0),
	PetersburgBlock:         big.NewInt(0),
	IstanbulBlock:           big.NewInt(0),
	MuirGlacierBlock:        big.NewInt(0),
	BerlinBlock:             big.NewInt(0),
	LondonBlock:             big.NewInt(0),
	ArrowGlacierBlock:       big.NewInt(0),
	GrayGlacierBlock:        big.NewInt(0),
	TerminalTotalDifficulty: big.NewInt(0), // 58_750_000_000_000_000_000_000
	ShanghaiTime:            newUint64(0),
	CancunTime:              newUint64(0),
	PragueTime:              newUint64(0),
	Ethash:                  new(params.EthashConfig),
}

type LEVM struct {
	allDBForState *database.AllDBForState
	evm           *vm.EVM
}

// New creates a new instace of the LEVM
func New(stateDBConfig *database.StateDBConfig1165, blockNumber *big.Int, stateRoot common.Hash, origin common.Address) *LEVM {
	// create blank LEVM instance:
	lvm := LEVM{}
	var err error
	// setup storage using dbpath
	lvm.allDBForState, err = database.NewAllDBForState(stateDBConfig, blockNumber, stateRoot, false, false)
	tools.PanicError("NewAllDBForState", err)
	// update the evm - creates new EVM
	lvm.NewEVM(origin, lvm.allDBForState.StateDB, lvm.allDBForState.DiskDB)

	return &lvm
}

func (lvm *LEVM) AllDB() *database.AllDBForState {
	return lvm.allDBForState
}
func newUint64(val uint64) *uint64 { return &val }

// NewEVM creates a fresh evm instance with
// new origin and blocknumber and time.
// This method recreates the contained EVM while
// keeping the stateDB the same.
func (lvm *LEVM) NewEVM(origin common.Address, statedb vm.StateDB, chiandb ethdb.Database) {

	// create context for the evm context

	chainContext := vminterface.NewChainContext(origin)
	blockContext := core.NewEVMBlockContext(chainContext.GetHeader(common.Hash{}, 0), chiandb, nil)
	//vmContext := vminterface.NewVMContext(origin, blockNumber, chainContext, lvm.allDBForState.DiskDB)

	lvm.evm = vm.NewEVM(blockContext, vm.TxContext{}, statedb, NewChainConfig, vm.Config{})
}

// DeployContract will create and deploy a new
// contract from the contract data.
func (lvm *LEVM) DeployContract(fromAddr common.Address, contractCode []byte) ([]byte, common.Address, uint64, error) {
	// Get reference to the transaction sender
	contractRef := vm.AccountRef(fromAddr)
	leftOver := big.NewInt(0)
	return lvm.evm.Create(
		contractRef,
		contractCode,
		lvm.allDBForState.StateDB.GetBalance(fromAddr).Uint64(),
		leftOver,
	)
}

// CallContract - make a call to a Contract Method
// using prepacked Inputs. To use ABI directly try
// lvm.CallContractABI()
func (lvm *LEVM) CallContract(callerAddr, contractAddr common.Address, inputs []byte, value *big.Int) ([]byte, error) {
	contractRef := vm.AccountRef(callerAddr)
	// Get reference to the transaction sender
	output, _, err := lvm.evm.Call(
		contractRef,
		contractAddr,
		inputs,
		lvm.allDBForState.StateDB.GetBalance(callerAddr).Uint64(),
		value,
	)
	//lvm.allDBForState.StateDB.SetBalance(callerAddr, big.NewInt(int64(gas)))
	return output, err
}

// CallContractABI - make a call to a Contract Method
// using the ABI.
func (lvm *LEVM) CallContractABI(callerAddr, contractAddr common.Address, value *big.Int, abiObject abi.ABI, funcName string, args ...interface{}) ([]byte, error) {

	inputs, err := abiObject.Pack(funcName, args...)
	//fmt.Println(common.Bytes2Hex(inputs))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	callerRef := vm.AccountRef(callerAddr)
	output, _, err := lvm.evm.Call(
		callerRef,
		contractAddr,
		inputs,
		lvm.allDBForState.StateDB.GetBalance(callerAddr).Uint64(),
		value,
	)
	//lvm.allDBForState.StateDB.SetBalance(callerAddr, big.NewInt(int64(gas)))
	return output, err
}

func PreReadState(blockTxs []types.Transactions, levm *LEVM, stmStateDB *state.StmStateDB) {
	//tools.PreReadState = true
	//defer func() { tools.PreReadState = false }()
	abiObject, _, err := tools.LoadContract(tools.ContractBasePath+"smallbank_fibonacci.abi", tools.ContractBasePath+"smallbank_fibonacci.bin")
	tools.PanicError("GenerateSmallBankTxs LoadContract ", err)
	levm.allDBForState.StateDB.GetOrNewStateObject(tools.ContractAddress)
	levm.allDBForState.StateDB.GetCode(tools.ContractAddress)

	stmStateDB.ReadStateVersion(tools.ContractAddress)
	stmStateDB.ReadStorageVersion(tools.ContractAddress, common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"), -1)
	addressSet := make(map[common.Address]struct{})

	var insertAddress = func(addr common.Address) {
		if _, exists := addressSet[addr]; !exists {
			addressSet[addr] = struct{}{}
		}
	}
	for _, txs := range blockTxs {
		for _, tx := range txs {
			//fmt.Println(tx.TxType)
			from := tx.From()
			to := tx.SmallBankTo
			insertAddress(*from)
			insertAddress(to)
		}
	}

	// 预读取账户状态
	for addr := range addressSet {
		_ = levm.allDBForState.StateDB.GetOrNewStateObject(addr)
		stmStateDB.ReadStateVersion(addr)

		tools.CatStorageState = true
		_, err := levm.CallContractABI(addr, tools.ContractAddress, new(big.Int).SetUint64(0), abiObject,
			"getBalance", addr)

		tools.PanicError("PreReadState CallContractABI to", err)
		tools.CatStorageState = false
		//fmt.Println()
		for _, hash := range tools.SlotHash {
			//fmt.Println(addr, hash)
			stmStateDB.ReadStorageVersion(tools.ContractAddress, hash, -1)
		}
		tools.SlotHash = tools.SlotHash[:0]
		//fmt.Println("finish account:")
	}
}

func (lvm *LEVM) CallContractABIWithStateDB(callerAddr, contractAddr common.Address, value *big.Int, statedb *state.StmTransaction, abiObject abi.ABI, funcName string, args ...interface{}) ([]byte, error) {

	inputs, err := abiObject.Pack(funcName, args...)
	//fmt.Println(common.Bytes2Hex(inputs))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	balance := statedb.GetBalance(callerAddr)
	callerRef := vm.AccountRef(callerAddr)
	output, _, err := lvm.evm.Call(
		callerRef,
		contractAddr,
		inputs,
		balance.Uint64(),
		value,
	)
	return output, err
}
