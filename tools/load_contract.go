package tools

import (
	"io/ioutil"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	ContractAddress = common.HexToAddress("0x48c8980037806eD4284D3353e2D6fC9e4256AC0A")
	StateRoot       = common.HexToHash("0x3c4f0c089974d4c6aabda0370494c8a09be6990cbc875f9b9fa2b5a03cc4384a")
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
