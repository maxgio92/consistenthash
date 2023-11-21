package consistenthash

import (
	"errors"
	"hash/crc32"
	"sort"
	"sync"
)

var ErrNodeNotFound = errors.New("node not found")

// Ring is a network of distributed nodes.
type Ring struct {
	Nodes Nodes

	sync.RWMutex
}

type Nodes []*Node

// Node is a single entity in a ring.
type Node struct {
	Id     string
	HashId uint32
}

func NewRing() *Ring {
	return &Ring{Nodes: Nodes{}}
}

func (r *Ring) AddNode(id string) {
	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)

	sort.Sort(r.Nodes)
}

func (r *Ring) RemoveNode(id string) error {
	r.Lock()
	defer r.Unlock()

	i := r.search(id)
	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

// Get returns the node closest to the key specified as argument,
// from the nodes ring.
func (r *Ring) Get(key string) string {
	r.RLock()
	defer r.RUnlock()

	i := r.search(key)
	if i >= r.Nodes.Len() {
		i = 0
	}

	return r.Nodes[i].Id
}

// search is a binary search in the ring.
// It's implemented with sort.Search binary search.
func (r *Ring) search(id string) int {
	searchFn := func(i int) bool {
		return r.Nodes[i].HashId >= checksum(id)
	}

	return sort.Search(r.Nodes.Len(), searchFn)
}

func NewNode(id string) *Node {
	return &Node{
		Id:     id,
		HashId: checksum(id),
	}
}

// Nodes implement sort.Interface.
func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Less(i, j int) bool { return n[i].HashId < n[j].HashId }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

func checksum(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
