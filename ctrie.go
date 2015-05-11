package matchbox

import (
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/Workiva/go-datastructures/queue"
)

const hasherPoolSize = 16

// ctrie is a concurrent, lock-free trie.
type ctrie struct {
	root       *iNode
	config     *Config
	hasherPool *queue.RingBuffer
}

// iNode is an indirection node. I-nodes remain present in the ctrie even as
// nodes above and below change. Thread-safety is achieved in part by
// performing CAS operations on the I-node instead of the internal node array.
type iNode struct {
	main *mainNode
}

type mainNode struct {
	cNode *cNode
	tNode *tNode
}

type cNode struct {
	branches map[string]*branch
}

func newCNode(keys []string, sub Subscriber) *cNode {
	if len(keys) == 1 {
		return &cNode{branches: map[string]*branch{
			keys[0]: &branch{subs: map[string]Subscriber{sub.ID(): sub}}}}
	}
	nin := &iNode{&mainNode{cNode: newCNode(keys[1:], sub)}}
	return &cNode{branches: map[string]*branch{keys[0]: &branch{subs: map[string]Subscriber{}, iNode: nin}}}
}

// inserted returns a copy of this C-node with the specified Subscriber
// inserted.
func (c *cNode) inserted(keys []string, sub Subscriber) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, branch := range c.branches {
		branches[key] = branch
	}
	var br *branch
	if len(keys) == 1 {
		br = &branch{subs: map[string]Subscriber{sub.ID(): sub}}
	} else {
		br = &branch{
			subs:  map[string]Subscriber{},
			iNode: &iNode{&mainNode{cNode: newCNode(keys[1:], sub)}},
		}
	}
	branches[keys[0]] = br
	return &cNode{branches: branches}
}

// updatedBranch returns a copy of this C-node with the specified branch
// updated.
func (c *cNode) updatedBranch(key string, in *iNode, br *branch) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, branch := range c.branches {
		branches[key] = branch
	}
	branches[key] = br.updated(in)
	return &cNode{branches: branches}
}

// updated returns a copy of this C-node with the specified branch updated.
func (c *cNode) updated(key string, sub Subscriber) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, branch := range c.branches {
		branches[key] = branch
	}
	newBranch := &branch{subs: map[string]Subscriber{sub.ID(): sub}}
	br, ok := branches[key]
	if ok {
		for id, sub := range br.subs {
			newBranch.subs[id] = sub
		}
	}
	branches[key] = newBranch
	return &cNode{branches: branches}
}

// tNode is tomb node which is a special node used to ensure proper ordering
// during removals.
type tNode struct {
	// TODO
}

// branch is either an iNode or sNode.
type branch struct {
	iNode *iNode
	subs  map[string]Subscriber
}

// updated returns a copy of this branch updated with the given I-node.
func (b *branch) updated(in *iNode) *branch {
	subs := make(map[string]Subscriber, len(b.subs))
	for id, sub := range b.subs {
		subs[id] = sub
	}
	return &branch{subs: subs, iNode: in}
}

func (b *branch) subscribers() []Subscriber {
	subs := make([]Subscriber, len(b.subs))
	i := 0
	for _, sub := range b.subs {
		subs[i] = sub
		i++
	}
	return subs
}

func newCtrie(config *Config) *ctrie {
	root := &iNode{main: &mainNode{cNode: &cNode{}}}
	return &ctrie{root: root, config: config}
}

func (c *ctrie) Insert(topic string, sub Subscriber) {
	keys := strings.Split(topic, c.config.Delimiter)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	if !iinsert(root, keys, sub, nil) {
		c.Insert(topic, sub)
	}
}

func (c *ctrie) Lookup(topic string) []Subscriber {
	keys := strings.Split(topic, c.config.Delimiter)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	result, ok := ilookup(root, keys, nil)
	if !ok {
		return c.Lookup(topic)
	}
	return result
}

func iinsert(i *iNode, keys []string, sub Subscriber, parent *iNode) bool {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		cn := main.cNode
		if br, ok := cn.branches[keys[0]]; !ok {
			// If the relevant bit is not in the map, a copy of the C-node with
			// the new entry is created. The linearization point is a
			// successful CAS.
			ncn := &mainNode{cNode: cn.inserted(keys, sub)}
			return atomic.CompareAndSwapPointer(
				mainPtr, unsafe.Pointer(main), unsafe.Pointer(ncn))
		} else {
			// If the relevant key is present in the map, its corresponding
			// branch is read.
			if len(keys) > 1 {
				// If more than 1 key is present in the path, the tree must be
				// traversed deeper.
				if br.iNode != nil {
					// If the branch has an I-node, iinsert is called
					// recursively.
					return iinsert(br.iNode, keys[1:], sub, i)
				}
				// Otherwise, an I-Node which points to a new C-node must be
				// added. The linearization point is a successful CAS.
				nin := &iNode{&mainNode{cNode: newCNode(keys[1:], sub)}}
				ncn := &mainNode{cNode: cn.updatedBranch(keys[0], nin, br)}
				return atomic.CompareAndSwapPointer(
					mainPtr, unsafe.Pointer(main), unsafe.Pointer(ncn))
			}
			// Insert the Subscriber by copying the C-node and updating the
			// respective branch. The linearization point is a successful CAS.
			ncn := &mainNode{cNode: cn.updated(keys[0], sub)}
			return atomic.CompareAndSwapPointer(
				mainPtr, unsafe.Pointer(main), unsafe.Pointer(ncn))
		}
	case main.tNode != nil:
		// TODO
		return false
	default:
		panic("Ctrie is in an invalid state")
	}
}

func ilookup(i *iNode, keys []string, parent *iNode) ([]Subscriber, bool) {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		cn := main.cNode
		if br, ok := cn.branches[keys[0]]; !ok {
			// If a branch doesn't exist for the key, no subscribers exist.
			return nil, true
		} else {
			// Otherwise, the relevant branch is read.
			if len(keys) > 1 {
				// If more than 1 key is present in the path, the tree must be
				// traversed deeper.
				if br.iNode == nil {
					// If the branch doesn't point to an I-node, no subscribers
					// exist.
					return nil, true
				}
				// If the branch has an I-node, ilookup is called recursively.
				return ilookup(br.iNode, keys[1:], i)
			}
			// Retrieve the subscribers from the branch.
			return br.subscribers(), true
		}
	default:
		panic("Ctrie is in an invalid state")
	}
}
