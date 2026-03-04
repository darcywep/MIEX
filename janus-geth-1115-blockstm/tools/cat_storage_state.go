package tools

import "github.com/ethereum/go-ethereum/common"

var (
	CatStorageState               = false
	JournalNonce                  = false
	PreReadState                  = false
	SlotHash        []common.Hash = make([]common.Hash, 0)
	StateModified                 = false
)
