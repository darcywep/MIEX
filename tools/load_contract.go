package tools

import (
	"io/ioutil"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	ContractAddress = common.HexToAddress("0x740F503Fa0eEb9A68ADa33304062384F0F0d45AE")
	StateRoot       = common.HexToHash("0xfd3aa1ec81e923b061f1314b40125176a4dfe2ed9cfabe7cdacf44901bd76036")
)

// LoadContract will open and decode a contracts
// Application Blockchain Interface and Binary files.
func LoadContract(abiPath, binPath string) (abi.ABI, []byte, error) {

	// load ABI
	abiFile, err := os.Open(abiPath)
	if err != nil {
		return abi.ABI{}, nil, err
	}
	abiObject, err := abi.JSON(abiFile)
	if err != nil {
		return abiObject, nil, err
	}

	//load and decode bin
	binRaw, err := ioutil.ReadFile(binPath)
	if err != nil {
		return abiObject, nil, err
	}
	binData, err := hexutil.Decode("0x" + string(binRaw))

	return abiObject, binData, err
}
