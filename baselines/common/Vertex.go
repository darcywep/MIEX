package common

import (
	"fmt"
	"sync"
)

type ChildVertex struct {
	Vertex     *Vertex
	Dependency DependencyType
}

type Vertex struct {
	*Transaction
	mu sync.RWMutex

	HyperId       int
	HyperVertex   *HyperVertex
	ID            string
	Layer         int
	Cost          int
	SelfCost      int
	Degree        int
	IsNested      bool
	HasStrong     bool
	ScheduledTime int

	CascadeVertices map[*Vertex]bool
	ReadSet         map[string]bool
	WriteSet        map[string]bool
	AllReadSet      map[string]bool
	AllWriteSet     map[string]bool
	Children        map[ChildVertex]bool
	StrongChildren  map[*Vertex]bool
	StrongParent    *Vertex
	ShouldWait      *Vertex

	DependenciesIn  map[*Vertex]bool
	DependenciesOut map[*Vertex]bool
}

func NewVertex(hyperVertex *HyperVertex, hyperId int, id string, layer int, isNested bool) *Vertex {
	v := &Vertex{
		Transaction:     NewTransaction(hyperVertex),
		HyperId:         hyperId,
		HyperVertex:     hyperVertex,
		ID:              id,
		Layer:           layer,
		IsNested:        isNested,
		CascadeVertices: make(map[*Vertex]bool),
		ReadSet:         make(map[string]bool),
		WriteSet:        make(map[string]bool),
		AllReadSet:      make(map[string]bool),
		AllWriteSet:     make(map[string]bool),
		Children:        make(map[ChildVertex]bool),
		StrongChildren:  make(map[*Vertex]bool),
		DependenciesIn:  make(map[*Vertex]bool),
		DependenciesOut: make(map[*Vertex]bool),
	}

	// 将自己加入级联集合
	v.CascadeVertices[v] = true
	return v
}

func (v *Vertex) GetChildren() map[ChildVertex]bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := make(map[ChildVertex]bool)
	for child := range v.Children {
		result[child] = true
	}
	return result
}

func (v *Vertex) AddChild(child *Vertex, dependency DependencyType) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.Children[ChildVertex{Vertex: child, Dependency: dependency}] = true
}

func (v *Vertex) PrintVertex() {
	v.mu.RLock()
	defer v.mu.RUnlock()

	fmt.Printf("VertexId: %s", v.ID)
	fmt.Printf(" Cost: %d", v.Cost)
	fmt.Printf(" IsNested: %t\n", v.IsNested)

	fmt.Print("ReadSet: ")
	for read := range v.ReadSet {
		fmt.Printf("%s ", read)
	}
	fmt.Println()

	fmt.Print("WriteSet: ")
	for write := range v.WriteSet {
		fmt.Printf("%s ", write)
	}
	fmt.Println()

	fmt.Print("CascadeVertices: ")
	for cascade := range v.CascadeVertices {
		fmt.Printf("%s ", cascade.ID)
	}
	fmt.Println()

	fmt.Println("Children: ")
	for child := range v.Children {
		fmt.Printf("Dependency: %s\n", v.DependencyTypeToString(child.Dependency))
		child.Vertex.PrintVertex()
	}
}

func (v *Vertex) DependencyTypeToString(typ DependencyType) string {
	switch typ {
	case STRONG:
		return "STRONG"
	case WEAK:
		return "WEAK"
	default:
		return "UNKNOWN"
	}
}

func (v *Vertex) Execute() {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.getHandler != nil {
		v.getHandler(v.ReadSet)
	}
	if v.setHandler != nil {
		v.setHandler(v.WriteSet, "value")
	}
	Exec(v.SelfCost)
}

func (v *Vertex) CountOverheads() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.SelfCost
}
