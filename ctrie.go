package matchbox

import (
	"strings"
	"sync/atomic"
	"unsafe"
)

// ctrie is a concurrent, lock-free trie.
type ctrie struct {
	root     *iNode
	config   *Config
	readOnly bool
}

// generation demarcates ctrie snapshots. We use a heap-allocated reference
// instead of an integer to avoid integer overflows.
type generation struct{}

// iNode is an indirection node. I-nodes remain present in the ctrie even as
// nodes above and below change. Thread-safety is achieved in part by
// performing CAS operations on the I-node instead of the internal node array.
type iNode struct {
	main *mainNode
	gen  *generation
}

// copyToGen returns a copy of this I-node copied to the given generation.
func (i *iNode) copyToGen(gen *generation, ctrie *ctrie) *iNode {
	nin := &iNode{gen: gen}
	main := gcasRead(i, ctrie)
	atomic.StorePointer(
		(*unsafe.Pointer)(unsafe.Pointer(&nin.main)), unsafe.Pointer(main))
	return nin
}

// mainNode is either a C-node or T-node to which an I-node points.
type mainNode struct {
	cNode  *cNode
	tNode  *tNode
	failed *mainNode

	// prev is set as a failed main node when we attempt to CAS and the
	// I-node's generation does not match the root generation. This signals
	// that the GCAS failed and the I-node's main node must be set back to the
	// previous value.
	prev *mainNode
}

// cNode is an internal main node containing a map of branches keyed on
// subscription components.
type cNode struct {
	branches map[string]*branch
	gen      *generation
}

// newCNode creates a new C-node with the given subscription path.
func newCNode(keys []string, sub Subscriber, gen *generation) *cNode {
	if len(keys) == 1 {
		return &cNode{
			branches: map[string]*branch{
				keys[0]: &branch{subs: map[string]Subscriber{sub.ID(): sub}}},
			gen: gen,
		}
	}
	nin := &iNode{main: &mainNode{cNode: newCNode(keys[1:], sub, gen)}, gen: gen}
	return &cNode{
		branches: map[string]*branch{
			keys[0]: &branch{subs: map[string]Subscriber{}, iNode: nin}},
		gen: gen,
	}
}

// inserted returns a copy of this C-node with the specified Subscriber
// inserted.
func (c *cNode) inserted(keys []string, sub Subscriber, gen *generation) *cNode {
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
			iNode: &iNode{main: &mainNode{cNode: newCNode(keys[1:], sub, gen)}, gen: gen},
		}
	}
	branches[keys[0]] = br
	return &cNode{branches: branches, gen: gen}
}

// updatedBranch returns a copy of this C-node with the specified branch
// updated.
func (c *cNode) updatedBranch(key string, in *iNode, br *branch, gen *generation) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, branch := range c.branches {
		branches[key] = branch
	}
	branches[key] = br.updated(in)
	return &cNode{branches: branches, gen: gen}
}

// updated returns a copy of this C-node with the specified branch updated.
func (c *cNode) updated(key string, sub Subscriber, gen *generation) *cNode {
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
	return &cNode{branches: branches, gen: gen}
}

// removed returns a copy of this C-node with the Subscriber removed from the
// corresponding branch.
func (c *cNode) removed(key string, sub Subscriber, gen *generation) *cNode {
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
	return &cNode{branches: branches, gen: gen}
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

// renewed returns a copy of this cNode with the I-nodes below it copied to the
// given generation.
func (c *cNode) renewed(gen *generation, ctrie *ctrie) *cNode {
	branches := make(map[string]*branch, len(c.branches))
	for key, br := range c.branches {
		if br.iNode != nil {
			branches[key] = &branch{iNode: br.iNode.copyToGen(gen, ctrie), subs: br.subs}
		} else {
			branches[key] = br
		}
	}
	return &cNode{branches: branches, gen: gen}
}

// tNode is tomb node which is a special node used to ensure proper ordering
// during removals.
type tNode struct{}

// branch contains subscribers and, optionally, points to an I-node.
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

// subscribers returns the Subscribers for this branch.
func (b *branch) subscribers() []Subscriber {
	subs := make([]Subscriber, len(b.subs))
	i := 0
	for _, sub := range b.subs {
		subs[i] = sub
		i++
	}
	return subs
}

// newCtrie creates a new ctrie with the given Config.
func newCtrie(config *Config) *ctrie {
	root := &iNode{main: &mainNode{cNode: &cNode{}}}
	return initCtrie(config, root, false)
}

// initCtrie creates a new ctrie with the given root and Config.
func initCtrie(config *Config, root *iNode, readOnly bool) *ctrie {
	return &ctrie{root: root, config: config, readOnly: readOnly}
}

// Insert adds the Subscriber to the ctrie for the given topic.
func (c *ctrie) Insert(topic string, sub Subscriber) {
	c.assertReadWrite()
	keys := strings.Split(topic, c.config.Delimiter)
	keys = c.config.reduceZeroOrMoreWildcards(keys)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	if !c.iinsert(root, keys, sub, nil, root.gen) {
		c.Insert(topic, sub)
	}
}

// Lookup returns the Subscribers for the given topic.
func (c *ctrie) Lookup(topic string) []Subscriber {
	keys := strings.Split(topic, c.config.Delimiter)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	result, ok := c.ilookup(root, keys, nil, false, root.gen)
	if !ok {
		return c.Lookup(topic)
	}
	return result
}

// Remove will remove the Subscriber from the topic if it is subscribed.
func (c *ctrie) Remove(topic string, sub Subscriber) {
	c.assertReadWrite()
	keys := strings.Split(topic, c.config.Delimiter)
	keys = c.config.reduceZeroOrMoreWildcards(keys)
	rootPtr := (*unsafe.Pointer)(unsafe.Pointer(&c.root))
	root := (*iNode)(atomic.LoadPointer(rootPtr))
	if !c.iremove(root, keys, sub, nil, root.gen) {
		c.Remove(topic, sub)
	}
}

// Snapshot returns a stable, point-in-time snapshot of the ctrie.
func (c *ctrie) Snapshot() *ctrie {
	for {
		root := c.readRoot()
		main := gcasRead(root, c)
		if c.rdcssRoot(root, main, root.copyToGen(&generation{}, c)) {
			return initCtrie(c.config, root.copyToGen(&generation{}, c), c.readOnly)
		}
	}
}

// ReadOnlySnapshot returns a stable, point-in-time snapshot of the ctrie which
// is read-only. Write operations on a read-only snapshot will panic.
func (c *ctrie) ReadOnlySnapshot() *ctrie {
	if c.readOnly {
		return c
	}
	for {
		root := c.readRoot()
		main := gcasRead(root, c)
		if c.rdcssRoot(root, main, root.copyToGen(&generation{}, c)) {
			return initCtrie(c.config, root, true)
		}
	}
}

func (c *ctrie) assertReadWrite() {
	if c.readOnly {
		panic("Cannot modify read-only snapshot")
	}
}

// iinsert attempts to add the Subscriber to the key path. True is returned if
// the Subscriber was added, false if the operation needs to be retried.
func (c *ctrie) iinsert(i *iNode, keys []string, sub Subscriber, parent *iNode, startGen *generation) bool {
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
			rn := cn
			if cn.gen != i.gen {
				rn = cn.renewed(i.gen, c)
			}
			ncn := &mainNode{cNode: rn.inserted(keys, sub, i.gen)}
			return gcas(i, main, ncn, c)
		} else {
			// If the relevant key is present in the map, its corresponding
			// branch is read.
			if len(keys) > 1 {
				// If more than 1 key is present in the path, the tree must be
				// traversed deeper.
				if br.iNode != nil {
					// If the branch has an I-node, iinsert is called
					// recursively.
					if startGen == br.iNode.gen {
						return c.iinsert(br.iNode, keys[1:], sub, i, startGen)
					}
					if gcas(i, main, &mainNode{cNode: cn.renewed(startGen, c)}, c) {
						return c.iinsert(i, keys, sub, parent, startGen)
					}
					return false
				}
				// Otherwise, an I-node which points to a new C-node must be
				// added. The linearization point is a successful CAS.
				rn := cn
				if cn.gen != i.gen {
					rn = cn.renewed(i.gen, c)
				}
				nin := &iNode{main: &mainNode{cNode: newCNode(keys[1:], sub, i.gen)}, gen: i.gen}
				ncn := &mainNode{cNode: rn.updatedBranch(keys[0], nin, br, i.gen)}
				return gcas(i, main, ncn, c)
			}
			if _, ok := br.subs[sub.ID()]; ok {
				// Already subscribed.
				return true
			}
			// Insert the Subscriber by copying the C-node and updating the
			// respective branch. The linearization point is a successful CAS.
			ncn := &mainNode{cNode: cn.updated(keys[0], sub, i.gen)}
			return gcas(i, main, ncn, c)
		}
	case main.tNode != nil:
		clean(parent)
		return false
	default:
		panic("Ctrie is in an invalid state")
	}
}

// iremove attempts to remove the Subscriber from the key path. True is
// returned if the Subscriber was removed (or didn't exist), false if the
// operation needs to be retried.
func (c *ctrie) iremove(i *iNode, keys []string, sub Subscriber, parent *iNode, startGen *generation) bool {
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
					if c.readOnly || startGen == br.iNode.gen {
						return c.iremove(br.iNode, keys[1:], sub, i, startGen)
					}
					if gcas(i, main, &mainNode{cNode: cn.renewed(startGen, c)}, c) {
						return c.iremove(i, keys, sub, parent, startGen)
					}
				}
				// Otherwise, the subscription doesn't exist.
				return true
			}
			if _, ok := br.subs[sub.ID()]; !ok {
				// Not subscribed.
				return true
			}
			// Remove the Subscriber by copying the C-node without it. A
			// contraction of the copy is then created. A successful CAS will
			// substitute the old C-node with the copied C-node, thus removing
			// the Subscriber from the trie - this is the linearization point.
			ncn := cn.removed(keys[0], sub, i.gen)
			cntr := c.toContracted(ncn, i)
			if gcas(i, main, cntr, c) {
				if parent != nil {
					main = gcasRead(i, c)
					if main.tNode != nil {
						cleanParent(parent, i, c, keys[0], startGen)
					}
				}
				return true
			}
			return false
		}
	case main.tNode != nil:
		clean(parent)
		return false
	default:
		panic("Ctrie is in an invalid state")
	}
}

// ilookup attempts to retrieve the Subscribers for the key path. True is
// returned if the Subscribers were retrieved, false if the operation needs to
// be retried.
func (c *ctrie) ilookup(i *iNode, keys []string, parent *iNode, zeroOrMore bool, startGen *generation) ([]Subscriber, bool) {
	// Linearization point.
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	switch {
	case main.cNode != nil:
		// Traverse exact-match branch, single-word-wildcard branch, and
		// zero-or-more-wildcard branch.
		exact, singleWC, zomWC := main.cNode.getBranches(keys[0], c.config)
		subs := map[string]Subscriber{}
		if exact != nil {
			s, ok := c.bLookup(i, parent, main, exact, keys, false, startGen)
			if !ok {
				return nil, false
			}
			for _, sub := range s {
				subs[sub.ID()] = sub
			}
		}
		if singleWC != nil {
			s, ok := c.bLookup(i, parent, main, singleWC, keys, false, startGen)
			if !ok {
				return nil, false
			}
			for _, sub := range s {
				subs[sub.ID()] = sub
			}
		}
		if zomWC != nil {
			s, ok := c.bLookup(i, parent, main, zomWC, keys, true, startGen)
			if !ok {
				return nil, false
			}
			for _, sub := range s {
				subs[sub.ID()] = sub
			}
		}
		if zeroOrMore && len(keys) > 1 && exact == nil && singleWC == nil && zomWC == nil {
			// Loopback on zero-or-more wildcard.
			return c.ilookup(i, keys[1:], parent, true, startGen)
		}
		s := make([]Subscriber, len(subs))
		i := 0
		for _, sub := range subs {
			s[i] = sub
			i++
		}
		return s, true
	case main.tNode != nil:
		clean(parent)
		return nil, false
	default:
		panic("Ctrie is in an invalid state")
	}
}

// bLookup attempts to retrieve the Subscribers from the key path along the
// given branch. True is returned if the Subscribers were retrieved, false if
// the operation needs to be retried.
func (c *ctrie) bLookup(i, parent *iNode, main *mainNode, b *branch, keys []string,
	zeroOrMore bool, startGen *generation) ([]Subscriber, bool) {

	if len(keys) > 1 {
		// If more than 1 key is present in the path, the tree must be
		// traversed deeper.
		if b.iNode == nil {
			if zeroOrMore {
				// Loopback on zero-or-more wildcard.
				return c.bLookup(i, parent, main, b, keys[1:], true, startGen)
			}
			// If the branch doesn't point to an I-node, no subscribers
			// exist.
			return nil, true
		}
		// If the branch has an I-node, ilookup is called recursively.
		if c.readOnly || startGen == b.iNode.gen {
			return c.ilookup(b.iNode, keys[1:], i, zeroOrMore, startGen)
		}
		if gcas(i, main, &mainNode{cNode: main.cNode.renewed(startGen, c)}, c) {
			return c.ilookup(i, keys, parent, zeroOrMore, startGen)
		}
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

// getZeroOrMoreWildcardSubscribers returns the Subscribers on the I-node's
// C-node's zero-or-more-wildcard branch, if it exists.
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

// getSubscribers returns the Subscribers for the given key on the I-node's
// C-node, if it exists.
func (c *ctrie) getSubscribers(i *iNode, key string) []Subscriber {
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&i.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	subs := []Subscriber{}
	if main.cNode != nil {
		exact, singleWC, zomWC := main.cNode.getBranches(key, c.config)
		if exact != nil {
			subs = append(subs, exact.subscribers()...)
		}
		if singleWC != nil {
			subs = append(subs, singleWC.subscribers()...)
		}
		if zomWC != nil {
			subs = append(subs, zomWC.subscribers()...)
		}
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
			unsafe.Pointer(main), unsafe.Pointer(toCompressed(main.cNode)))
	}
}

// cleanParent reads the main node of the parent I-node p and the current
// I-node i and checks if the T-node below i is reachable from p. If i is no
// longer reachable, some other thread has already completed the contraction.
// If it is reachable, the C-node below p is replaced with its contraction.
func cleanParent(parent, i *iNode, c *ctrie, key string, startGen *generation) {
	var (
		mainPtr  = (*unsafe.Pointer)(unsafe.Pointer(&i.main))
		main     = (*mainNode)(atomic.LoadPointer(mainPtr))
		pMainPtr = (*unsafe.Pointer)(unsafe.Pointer(&parent.main))
		pMain    = (*mainNode)(atomic.LoadPointer(pMainPtr))
	)
	if pMain.cNode != nil {
		if br, ok := pMain.cNode.branches[key]; ok {
			if br.iNode != i {
				return
			}
			if main.tNode != nil {
				ncn := toCompressed(pMain.cNode)
				if !gcas(parent, pMain, c.toContracted(ncn.cNode, parent), c) &&
					c.readRoot().gen == startGen {
					cleanParent(parent, i, c, key, startGen)
				}
			}
		}
	}
}

// toCompressed prunes any branches to tombed I-nodes and returns the
// compressed main node.
func toCompressed(cn *cNode) *mainNode {
	branches := make(map[string]*branch, len(cn.branches))
	for key, br := range cn.branches {
		if !prunable(br) {
			branches[key] = br
		}
	}
	return &mainNode{cNode: &cNode{branches: branches, gen: cn.gen}}
}

// prunable indicates if the branch can be pruned. A branch can be pruned if
// it has no subscribers and points to nowhere or it has no subscribers and
// points to a tombed I-node.
func prunable(br *branch) bool {
	if len(br.subs) > 0 {
		return false
	}
	if br.iNode == nil {
		return true
	}
	mainPtr := (*unsafe.Pointer)(unsafe.Pointer(&br.iNode.main))
	main := (*mainNode)(atomic.LoadPointer(mainPtr))
	return main.tNode != nil
}

// gcas is a generation-compare-and-swap which has semantics similar to RDCSS,
// but it does not create the intermediate object except in the case of
// failures that occur due to the snapshot being taken. This ensures that the
// write occurs only if the Ctrie root generation has remained the same in
// addition to the I-node having the expected value.
func gcas(in *iNode, old, n *mainNode, ct *ctrie) bool {
	prevPtr := (*unsafe.Pointer)(unsafe.Pointer(&n.prev))
	atomic.StorePointer(prevPtr, unsafe.Pointer(old))
	if atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&in.main)),
		unsafe.Pointer(old), unsafe.Pointer(n)) {
		gcasComplete(in, n, ct)
		return atomic.LoadPointer(prevPtr) == nil
	}
	return false
}

// gcasRead performs a GCAS-linearizable read of the I-node's main node.
func gcasRead(in *iNode, ctrie *ctrie) *mainNode {
	m := (*mainNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&in.main))))
	prev := (*mainNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&m.prev))))
	if prev == nil {
		return m
	}
	return gcasComplete(in, m, ctrie)
}

// gcasComplete commits the GCAS operation.
func gcasComplete(i *iNode, m *mainNode, ctrie *ctrie) *mainNode {
	for {
		if m == nil {
			return nil
		}
		prev := (*mainNode)(atomic.LoadPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&m.prev))))
		root := ctrie.rdcssReadRoot(true)
		if prev == nil {
			return m
		}

		if prev.failed != nil {
			// Signals GCAS failure. Swap old value back into I-node.
			fn := prev.failed
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&i.main)),
				unsafe.Pointer(m), unsafe.Pointer(fn.prev)) {
				return fn.prev
			}
			m = (*mainNode)(atomic.LoadPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&i.main))))
			continue
		}

		if root.gen == i.gen && !ctrie.readOnly {
			// Commit GCAS.
			if atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&m.prev)), unsafe.Pointer(prev), nil) {
				return m
			}
			continue
		}

		// Generations did not match. Store failed node on prev to signal
		// I-node's main node must be set back to the previous value.
		atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&m.prev)),
			unsafe.Pointer(prev),
			unsafe.Pointer(&mainNode{failed: prev}))
		m = (*mainNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&i.main))))
		return gcasComplete(i, m, ctrie)
	}
}

// rdcssDescriptor is an intermediate struct which communicates the intent to
// replace the value in an I-node and check that the root's generation has not
// changed before committing to the new value.
type rdcssDescriptor struct {
	old       *iNode
	expected  *mainNode
	nv        *iNode
	committed bool
	token     int8
}

// HACK: see note in rdcssReadRoot.
const rdcssToken int8 = -1

// readRoot performs a linearizable read of the Ctrie root. This operation is
// prioritized so that if another thread performs a GCAS on the root, a
// deadlock does not occur.
func (c *ctrie) readRoot() *iNode {
	return c.rdcssReadRoot(false)
}

// rdcssReadRoot performs a RDCSS-linearizable read of the Ctrie root with the
// given priority.
func (c *ctrie) rdcssReadRoot(abort bool) *iNode {
	r := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.root)))
	desc := (*rdcssDescriptor)(r)
	// This is an awful and unsafe hack. Is there a better way to determine the
	// type of a pointer?
	if desc.token == rdcssToken {
		return c.rdcssComplete(abort)
	}
	return (*iNode)(r)
}

// rdcssRoot performs a RDCSS on the Ctrie root. This is used to create a
// snapshot of the Ctrie by copying the root I-node and setting it to a new
// generation.
func (c *ctrie) rdcssRoot(old *iNode, expected *mainNode, nv *iNode) bool {
	desc := &rdcssDescriptor{
		old:      old,
		expected: expected,
		nv:       nv,
		token:    rdcssToken,
	}
	if c.casRoot(unsafe.Pointer(old), unsafe.Pointer(desc)) {
		c.rdcssComplete(false)
		return *(*bool)(atomic.LoadPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&desc.committed))))
	}
	return false
}

// rdcssComplete commits the RDCSS operation.
func (c *ctrie) rdcssComplete(abort bool) *iNode {
	for {
		r := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.root)))
		desc := (*rdcssDescriptor)(r)
		if desc.token != rdcssToken {
			return (*iNode)(r)
		}

		var (
			ov  = desc.old
			exp = desc.expected
			nv  = desc.nv
		)

		if abort {
			if c.casRoot(unsafe.Pointer(desc), unsafe.Pointer(ov)) {
				return ov
			}
			continue
		}

		oldeMain := gcasRead(ov, c)
		if oldeMain == exp {
			// Commit the RDCSS.
			if c.casRoot(unsafe.Pointer(desc), unsafe.Pointer(nv)) {
				desc.committed = true
				return nv
			}
			continue
		}
		if c.casRoot(unsafe.Pointer(desc), unsafe.Pointer(ov)) {
			return ov
		}
		continue
	}
}

// casRoot performs a CAS on the ctrie root. The given pointers should either
// be rdcssDescriptors or iNodes.
func (c *ctrie) casRoot(ov, nv unsafe.Pointer) bool {
	c.assertReadWrite()
	return atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&c.root)), ov, nv)
}
