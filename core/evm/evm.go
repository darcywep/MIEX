package evm

import (
	"Janus/ethereum/config"
	"Janus/ethereum/core/tracing"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/params"

	"Janus/core/evm/vminterface"

	"Janus/ethereum/core/vm"

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
type LEVM struct {
	allDBForState *database.AllDBForState
	evm           *vm.EVM
}

// New creates a new instace of the LEVM
func New(stateDBConfig *database.StateDBConfig, blockNumber *big.Int, stateRoot common.Hash, origin common.Address) *LEVM {
	// create blank LEVM instance:
	lvm := LEVM{}
	var err error
	// setup storage using dbpath
	lvm.allDBForState, err = database.NewAllDBForState(stateDBConfig, blockNumber, stateRoot, false, false)
	tools.PanicError(err)
	// update the evm - creates new EVM
	lvm.NewEVM(blockNumber, origin)

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
func (lvm *LEVM) NewEVM(blockNumber *big.Int, origin common.Address) {

	// create context for the evm context
	chainContext := vminterface.NewChainContext(origin)
	vmContext := vminterface.NewVMContext(origin, blockNumber, chainContext)

	// create the evm
	newChainConfig := &params.ChainConfig{
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
		DepositContractAddress:  common.HexToAddress("0x00000000219ab540356cbb839cbe05303d7705fa"),
		Ethash:                  new(params.EthashConfig),
		BlobScheduleConfig: &params.BlobScheduleConfig{
			Cancun: params.DefaultCancunBlobConfig,
			Prague: params.DefaultPragueBlobConfig,
		},
	}
	lvm.evm = vm.NewEVM(vmContext, lvm.allDBForState.StateDB, newChainConfig, config.DefaultVmConfig)
}

// DeployContract will create and deploy a new
// contract from the contract data.
func (lvm *LEVM) DeployContract(fromAddr common.Address, contractCode []byte) ([]byte, common.Address, uint64, error) {
	return lvm.evm.Create(
		fromAddr,
		contractCode,
		lvm.allDBForState.StateDB.GetBalance(fromAddr).Uint64(),
		new(uint256.Int).SetUint64(0),
	)
}

// CallContract - make a call to a Contract Method
// using prepacked Inputs. To use ABI directly try
// lvm.CallContractABI()
func (lvm *LEVM) CallContract(callerAddr, contractAddr common.Address, inputs []byte, value *uint256.Int) ([]byte, error) {
	// Get reference to the transaction sender
	output, gas, err := lvm.evm.Call(
		callerAddr,
		contractAddr,
		inputs,
		lvm.allDBForState.StateDB.GetBalance(callerAddr).Uint64(),
		value,
	)
	lvm.allDBForState.StateDB.SetBalance(callerAddr, new(uint256.Int).SetUint64(gas), tracing.BalanceIncreaseGasReturn)
	return output, err
}

// CallContractABI - make a call to a Contract Method
// using the ABI.
func (lvm *LEVM) CallContractABI(callerAddr, contractAddr common.Address, value *uint256.Int, abiObject abi.ABI, funcName string, args ...interface{}) ([]byte, error) {

	inputs, err := abiObject.Pack(funcName, args...)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	output, gas, err := lvm.evm.Call(
		callerAddr,
		contractAddr,
		inputs,
		lvm.allDBForState.StateDB.GetBalance(callerAddr).Uint64(),
		value,
	)
	lvm.allDBForState.StateDB.SetBalance(callerAddr, new(uint256.Int).SetUint64(gas), tracing.BalanceIncreaseGasReturn)
	return output, err
}
