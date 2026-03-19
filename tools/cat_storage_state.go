package tools

import "sync"

var (
	CatStorageState        = false
	JournalNonce           = false
	Uint64          uint64 = 0xFFFFFFFFFFFFFFFF
)

var (
	TraceAbort      = false
	TraceAbortMutex sync.Mutex
)
