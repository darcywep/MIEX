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

var (
	// TraceHarmonyWorkerLog 控制 Harmony/newHarmony 内部 worker 级别的调试输出。
	// 默认关闭，避免真实负载实验中大量打印 execute/abort/fallback/cleanup 明细。
	TraceHarmonyWorkerLog = false
)
