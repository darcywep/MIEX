package common

type RWSets struct {
	ReadSet  map[*Vertex]bool
	WriteSet map[*Vertex]bool
}

func NewRWSets() *RWSets {
	return &RWSets{
		ReadSet:  make(map[*Vertex]bool),
		WriteSet: make(map[*Vertex]bool),
	}
}

// 模拟交易执行函数
func Exec(cost int) {
	// 实际执行逻辑
}

type DependencyType int

const (
	STRONG DependencyType = iota
	WEAK
)

type EdgeType int

const (
	NONE EdgeType = iota
	IN
	OUT
	BOTH
)

type TaskPriority int

const (
	HIGH_PRIORITY TaskPriority = iota
	LOW_PRIORITY
)

func EdgeTypeToString(typ EdgeType) string {
	switch typ {
	case IN:
		return "IN"
	case OUT:
		return "OUT"
	case BOTH:
		return "BOTH"
	default:
		return "UNKNOWN"
	}
}

func TaskPriorityToString(priority TaskPriority) string {
	switch priority {
	case HIGH_PRIORITY:
		return "HIGH_PRIORITY"
	case LOW_PRIORITY:
		return "LOW_PRIORITY"
	default:
		return "UNKNOWN"
	}
}

var BLOCK_SIZE int = 1000
var TX_NUM int = 2000
var IsOutputEnabled bool = false
