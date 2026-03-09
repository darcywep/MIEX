package tools

import (
	"io/ioutil"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	ContractAddress = common.HexToAddress("0x3F8b2AC9A5885430551DA4f9D90B734FF09D71C5")
	StateRoot       = common.HexToHash("0x2d0b20f7eb4cf0ee86073b184dff0d410063561032d4dd7a3c1eea3fd1f73319")
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
