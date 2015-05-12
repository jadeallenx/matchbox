package matchbox

import (
	"strings"
	"sync/atomic"
	"unsafe"
)

// ctrie is a concurrent, lock-free trie.
type ctrie struct {
	root   *iNode
	config *Config
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
	return &cNode{branches: map[string]*branch{
		keys[0]: &branch{subs: map[string]Subscriber{}, iNode: nin}},
	}
}

// inserted returns a copy of this C-node with the specified Subscriber
// inserted.
func (c *cNode) inserted(keys []string, sub Subscriber) *cNode {
	branches := make(map[string]*branch, len(c.branches)+1)
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

// removed returns a copy of this C-node with the Subscriber removed from the
// corresponding branch.
func (c *cNode) removed(key string, sub Subscriber) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, branch := range c.branches {
		branches[key] = branch
	}
	br, ok := branches[key]
	if ok {
		br = br.removed(sub)
		if len(br.subs) == 0 && br.iNode == nil {
			// Remove the branch if it contains no subscribers and doesn't
			// point anywhere.
			delete(branches, key)
		} else {
			branches[key] = br
		}
	}
	return &cNode{branches: branches}
}

// getBranches returns the branches for the given key. There are three
// possible branches: exact match, single wildcard, and zero-or-more wildcard.
func (c *cNode) getBranches(key string, config *Config) (*branch, *branch, *branch) {
	return c.getBranch(key), c.getBranch(config.SingleWildcard),
		c.getBranch(config.ZeroOrMoreWildcard)
}

// getBranch returns the branch for the given key or nil if one doesn't exist.
func (c *cNode) getBranch(key string) *branch {
	return c.branches[key]
}

// tNode is tomb node which is a special node used to ensure proper ordering
// during removals.
type tNode struct{}

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

// removed returns a copy of this branch with the given Subscriber removed.
func (b *branch) removed(sub Subscriber) *branch {
	subs := make(map[string]Subscriber, len(b.subs))
	for id, sub := range b.subs {
		subs[id] = sub
	}
	delete(subs, sub.ID())
	return &branch{subs: subs, iNode: b.iNode}
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
	keys = c.config.reduceZeroOrMoreWildcards(keys)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	if !c.iinsert(root, keys, sub, nil) {
		c.Insert(topic, sub)
	}
}

func (c *ctrie) Lookup(topic string) []Subscriber {
	keys := strings.Split(topic, c.config.Delimiter)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	result, ok := c.ilookup(root, keys, nil, false)
	if !ok {
		return c.Lookup(topic)
	}
	return result
}

func (c *ctrie) Remove(topic string, sub Subscriber) {
	keys := strings.Split(topic, c.config.Delimiter)
	keys = c.config.reduceZeroOrMoreWildcards(keys)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	if !c.iremove(root, keys, sub, nil) {
		c.Remove(topic, sub)
	}
}

func (c *ctrie) iinsert(i *iNode, keys []string, sub Subscriber, parent *iNode) bool {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		cn := main.cNode
		if br := cn.getBranch(keys[0]); br == nil {
			// If the relevant branch is not in the map, a copy of the C-node
			// with the new entry is created. The linearization point is a
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
					return c.iinsert(br.iNode, keys[1:], sub, i)
				}
				// Otherwise, an I-node which points to a new C-node must be
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
		clean(parent)
		return false
	default:
		panic("Ctrie is in an invalid state")
	}
}

func (c *ctrie) iremove(i *iNode, keys []string, sub Subscriber, parent *iNode) bool {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		cn := main.cNode
		if br := cn.getBranch(keys[0]); br == nil {
			// If the relevant key is not in the map, the subscription doesn't
			// exist.
			return true
		} else {
			// If the relevant key is present in the map, its corresponding
			// branch is read.
			if len(keys) > 1 {
				// If more than 1 key is present in the path, the tree must be
				// traversed deeper.
				if br.iNode != nil {
					// If the branch has an I-node, iremove is called
					// recursively.
					return c.iremove(br.iNode, keys[1:], sub, i)
				}
				// Otherwise, the subscription doesn't exist.
				return true
			}
			// Remove the Subscriber by copying the C-node without it. A
			// contraction of the copy is then created. A successful CAS will
			// substitute the old C-node with the copied C-node, thus removing
			// the Subscriber from the trie - this is the linearization point.
			ncn := cn.removed(keys[0], sub)
			cntr := c.toContracted(ncn, i)
			return atomic.CompareAndSwapPointer(
				mainPtr, unsafe.Pointer(main), unsafe.Pointer(cntr))
		}
	case main.tNode != nil:
		clean(parent)
		return false
	default:
		panic("Ctrie is in an invalid state")
	}
}

func (c *ctrie) ilookup(i *iNode, keys []string, parent *iNode, zeroOrMore bool) ([]Subscriber, bool) {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		// Traverse exact-match branch, single-word-wildcard branch, and
		// zero-or-more-wildcard branch.
		exact, singleWC, zomWC := main.cNode.getBranches(keys[0], c.config)
		subs := []Subscriber{}
		if exact != nil {
			s, ok := c.bLookup(i, exact, keys, false)
			if !ok {
				return nil, false
			}
			subs = append(subs, s...)
		}
		if singleWC != nil {
			s, ok := c.bLookup(i, singleWC, keys, false)
			if !ok {
				return nil, false
			}
			subs = append(subs, s...)
		}
		if zomWC != nil {
			s, ok := c.bLookup(i, zomWC, keys, true)
			if !ok {
				return nil, false
			}
			subs = append(subs, s...)
		}
		if zeroOrMore && exact == nil && singleWC == nil && zomWC == nil {
			// Loopback on zero-or-more wildcard.
			return c.ilookup(i, keys[1:], parent, true)
		}
		return subs, true
	case main.tNode != nil:
		clean(parent)
		return nil, false
	default:
		panic("Ctrie is in an invalid state")
	}
}

func (c *ctrie) bLookup(parent *iNode, b *branch, keys []string, zeroOrMore bool) ([]Subscriber, bool) {
	if len(keys) > 1 {
		// If more than 1 key is present in the path, the tree must be
		// traversed deeper.
		if b.iNode == nil {
			if zeroOrMore {
				// Loopback on zero-or-more wildcard.
				return c.bLookup(parent, b, keys[1:], true)
			}
			// If the branch doesn't point to an I-node, no subscribers
			// exist.
			return nil, true
		}
		// If the branch has an I-node, ilookup is called recursively.
		return c.ilookup(b.iNode, keys[1:], parent, zeroOrMore)
	}

	// Retrieve the subscribers from the branch.
	subscribers := b.subscribers()

	// Is there a zero-or-more wildcard following this node? If so, get its
	// subscribers.
	if b.iNode != nil {
		subscribers = append(subscribers,
			c.getZeroOrMoreWildcardSubscribers(b.iNode)...)
	}

	// Were we looping on a zero-or-more wildcard? If so, check for the tail
	// and get its subscribers.
	if zeroOrMore && b.iNode != nil {
		subscribers = append(subscribers, c.getSubscribers(b.iNode, keys[0])...)
	}

	return subscribers, true
}

func (c *ctrie) getZeroOrMoreWildcardSubscribers(i *iNode) []Subscriber {
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	if main.cNode != nil {
		if br := main.cNode.getBranch(c.config.ZeroOrMoreWildcard); br != nil {
			return br.subscribers()
		}
	}
	return nil
}

func (c *ctrie) getSubscribers(i *iNode, key string) []Subscriber {
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	exact, singleWC, zomWC := main.cNode.getBranches(key, c.config)
	subs := []Subscriber{}
	if exact != nil {
		subs = append(subs, exact.subscribers()...)
	}
	if singleWC != nil {
		subs = append(subs, singleWC.subscribers()...)
	}
	if zomWC != nil {
		subs = append(subs, zomWC.subscribers()...)
	}
	return subs
}

// toContracted ensures that every I-node except the root points to a C-node
// with at least one branch or a T-node. If a given C-node has no branches and
// is not at the root level, a T-node is returned.
func (c *ctrie) toContracted(cn *cNode, parent *iNode) *mainNode {
	if c.root != parent && len(cn.branches) == 0 {
		return &mainNode{tNode: &tNode{}}
	}
	return &mainNode{cNode: cn}
}

// clean replaces an I-node's C-node with a copy that has any tombed I-nodes
// resurrected.
func clean(i *iNode) {
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	if main.cNode != nil {
		atomic.CompareAndSwapPointer(mainPtr,
			unsafe.Pointer(main), unsafe.Pointer(resurrect(main.cNode)))
	}
}

// resurrect replaces any T-nodes with a resurrected I-node.
func resurrect(cn *cNode) *mainNode {
	branches := make(map[string]*branch, len(cn.branches))
	for key, br := range cn.branches {
		if br.iNode != nil {
			mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&br.iNode.main))
			main := (*mainNode)(atomic.LoadPointer(mainPtr))
			if main.tNode != nil {
				branches[key] = &branch{
					subs:  map[string]Subscriber{},
					iNode: &iNode{main: &mainNode{cNode: &cNode{}}},
				}
			} else {
				branches[key] = br
			}
		} else {
			branches[key] = br
		}
	}
	return &mainNode{cNode: &cNode{branches: branches}}
}
