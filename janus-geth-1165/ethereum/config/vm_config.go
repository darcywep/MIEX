package config

import (
	"janus-geth-1165/ethereum/core/vm"
)

//{Tracer:<nil> NoBaseFee:false EnablePreimageRecording:false ExtraEips:[] StatelessSelfValidation:false EnableWitnessStats:false

var DefaultVmConfig = vm.Config{
	Tracer:                  nil,
	NoBaseFee:               false,
	EnablePreimageRecording: false,
	ExtraEips:               make([]int, 0),
	StatelessSelfValidation: false,
	EnableWitnessStats:      false,
}
