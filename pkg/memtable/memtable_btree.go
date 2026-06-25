package memtable

import (
	"sync"
)

const (
	btreeLeafMaxItems      = 63
	btreeInternalMaxItems  = 63
	btreeLeafChunkSize     = 2048
	btreeInternalChunkSize = 128
	btreeMaxDataBytes      = 1 << 24 // 16 MiB
	btreeLeafBit           = 1 << 31
)

type btreeNodeRef uint32

func makeLeafRef(i uint32) btreeNodeRef {
	return btreeNodeRef(btreeLeafBit | i)
}

func makeInternalRef(i uint32) btreeNodeRef {
	return btreeNodeRef(i)
}

func (r btreeNodeRef) isLeaf() bool {
	return uint32(r)&btreeLeafBit != 0
}

func (r btreeNodeRef) index() uint32 {
	return uint32(r) &^ btreeLeafBit
}

type btreeKVEntry struct {
	keyOff uint32
	keyLen uint32
	valOff uint32
	valLen uint32
}

type btreeKeyRef struct {
	keyOff uint32
	keyLen uint32
}

type btreeLeafNode struct {
	n        uint16
	nextLeaf uint32

	items [btreeLeafMaxItems]btreeKVEntry
}

type btreeInternalNode struct {
	n uint16

	// Fence keys: each key is the max key in the child to its left.
	keys     [btreeInternalMaxItems]btreeKeyRef
	children [btreeInternalMaxItems + 1]btreeNodeRef
}

type btreeMemtable struct {
	mu        sync.RWMutex
	cmp       Comparator
	root      btreeNodeRef
	firstLeaf uint32
	lastLeaf  uint32

	// Index 0 is reserved as nil for both node arrays.
	leafChunks     []*[btreeLeafChunkSize]btreeLeafNode
	leafCount      uint32
	internalChunks []*[btreeInternalChunkSize]btreeInternalNode
	internalCount  uint32

	data    []byte
	dataLen int
	keyLen  int
}

func newBTreeMemtable(opts ...Option) Memtable {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	m := &btreeMemtable{
		cmp:            cfg.Comparator,
		leafChunks:     make([]*[btreeLeafChunkSize]btreeLeafNode, 0, 4),
		internalChunks: make([]*[btreeInternalChunkSize]btreeInternalNode, 0, 1),
		data:           make([]byte, btreeMaxDataBytes),
	}

	m.leafChunks = append(m.leafChunks, new([btreeLeafChunkSize]btreeLeafNode))
	m.internalChunks = append(m.internalChunks, new([btreeInternalChunkSize]btreeInternalNode))
	m.leafCount = 2     // leaf 0 = nil, leaf 1 = root
	m.internalCount = 1 // internal 0 = nil

	m.root = makeLeafRef(1)
	m.firstLeaf = 1
	m.lastLeaf = 1

	return m
}

func (m *btreeMemtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	leafIdx := m.findLeafLocked(key)
	leaf := m.leaf(leafIdx)

	pos := m.lowerBoundLeaf(leaf, key)
	if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
		return m.value(leaf.items[pos]), true
	}

	return nil, false
}

func (m *btreeMemtable) SeekGE(key []byte) ([]byte, []byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	leafIdx := m.findLeafLocked(key)
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)

	for leafIdx != 0 {
		leaf = m.leaf(leafIdx)

		if pos < int(leaf.n) {
			e := leaf.items[pos]
			return m.entryKey(e), m.value(e), true
		}

		leafIdx = leaf.nextLeaf
		pos = 0
	}

	return nil, nil, false
}

func (m *btreeMemtable) Set(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// An update only needs value bytes.
	if err := m.checkArenaCapacity(len(value)); err != nil {
		return err
	}

	insertFits := m.checkArenaCapacityForKV(len(key), len(value)) == nil

	// Right-edge fast path.
	if m.keyLen > 0 && m.lastLeaf != 0 {
		leaf := m.leaf(m.lastLeaf)

		if leaf.n > 0 {
			last := leaf.items[leaf.n-1]
			c := m.cmp(key, m.entryKey(last))

			if c == 0 {
				off, ln, err := m.appendBytesLocked(value)
				if err != nil {
					return err
				}

				leaf = m.leaf(m.lastLeaf)
				leaf.items[leaf.n-1].valOff = off
				leaf.items[leaf.n-1].valLen = ln
				return nil
			}

			if c > 0 && leaf.n < btreeLeafMaxItems {
				if !insertFits {
					return ErrMemtableFull
				}

				e, err := m.appendEntryLocked(key, value)
				if err != nil {
					return err
				}

				leaf = m.leaf(m.lastLeaf)
				leaf.items[leaf.n] = e
				leaf.n++
				m.keyLen++
				return nil
			}
		}
	}

	// If a new insert cannot fit, we can still update an existing key.
	// Do a single lookup and only allow update.
	if !insertFits {
		leafIdx := m.findLeafLocked(key)
		leaf := m.leaf(leafIdx)
		pos := m.lowerBoundLeaf(leaf, key)

		if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
			off, ln, err := m.appendBytesLocked(value)
			if err != nil {
				return err
			}

			leaf = m.leaf(leafIdx)
			leaf.items[pos].valOff = off
			leaf.items[pos].valLen = ln
			return nil
		}

		return ErrMemtableFull
	}

	// From here on, both update and new insert are safe from an arena-space
	// perspective. Structural splits can happen without risking a later
	// arena-capacity failure for this write.
	if m.isFull(m.root) {
		oldRoot := m.root

		newRoot, err := m.allocInternal()
		if err != nil {
			return err
		}

		m.root = makeInternalRef(newRoot)
		m.internal(newRoot).children[0] = oldRoot

		if err := m.splitChild(newRoot, 0); err != nil {
			return err
		}
	}

	nodeRef := m.root

	for {
		if nodeRef.isLeaf() {
			return m.insertIntoLeaf(nodeRef.index(), key, value)
		}

		nodeIdx := nodeRef.index()
		node := m.internal(nodeIdx)
		childPos := m.childIndex(node, key)
		childRef := node.children[childPos]

		if m.isFull(childRef) {
			if err := m.splitChild(nodeIdx, childPos); err != nil {
				return err
			}

			node = m.internal(nodeIdx)

			if m.cmp(key, m.refKey(node.keys[childPos])) > 0 {
				childPos++
			}

			childRef = node.children[childPos]
		}

		nodeRef = childRef
	}
}

// func (m *btreeMemtable) Delete(key []byte) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
//
// 	leafIdx := m.findLeafLocked(key)
// 	leaf := m.leaf(leafIdx)
//
// 	pos := m.lowerBoundLeaf(leaf, key)
// 	if pos >= int(leaf.n) || m.cmp(m.entryKey(leaf.items[pos]), key) != 0 {
// 		return
// 	}
//
// 	copy(leaf.items[pos:int(leaf.n)-1], leaf.items[pos+1:int(leaf.n)])
// 	leaf.items[leaf.n-1] = btreeKVEntry{}
// 	leaf.n--
// 	m.keyLen--
// }

func (m *btreeMemtable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.dataLen
}

func (m *btreeMemtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.keyLen
}

func (m *btreeMemtable) Iter() Iterator {
	return m.IterRange(nil, nil)
}

func (m *btreeMemtable) IterFrom(key []byte) Iterator {
	return m.IterRange(key, nil)
}

func (m *btreeMemtable) IterRange(start, end []byte) Iterator {
	return func(yield func([]byte, []byte) bool) {
		c := m.Cursor(start, end)
		for {
			k, v, ok := c.Next()
			if !ok {
				return
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

func (m *btreeMemtable) fillIterBatch(
	start []byte,
	leafIdx uint32,
	batch *[btreeLeafMaxItems]btreeKVEntry,
) (nextLeaf uint32, n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pos := 0
	if leafIdx == 0 {
		if start != nil {
			leafIdx = m.findLeafLocked(start)
			pos = m.lowerBoundLeaf(m.leaf(leafIdx), start)
		} else {
			leafIdx = m.firstLeaf
		}
	}

	if leafIdx == 0 {
		return 0, 0
	}

	leaf := m.leaf(leafIdx)
	n = copy(batch[:], leaf.items[pos:int(leaf.n)])

	return leaf.nextLeaf, n
}

func (m *btreeMemtable) insertIntoLeaf(leafIdx uint32, key, value []byte) error {
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)

	if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
		off, ln, err := m.appendBytesLocked(value)
		if err != nil {
			return err
		}

		leaf = m.leaf(leafIdx)
		leaf.items[pos].valOff = off
		leaf.items[pos].valLen = ln
		return nil
	}

	e, err := m.appendEntryLocked(key, value)
	if err != nil {
		return err
	}

	leaf = m.leaf(leafIdx)

	copy(leaf.items[pos+1:int(leaf.n)+1], leaf.items[pos:int(leaf.n)])
	leaf.items[pos] = e
	leaf.n++
	m.keyLen++

	return nil
}

func (m *btreeMemtable) splitChild(parentIdx uint32, childPos int) error {
	parent := m.internal(parentIdx)
	childRef := parent.children[childPos]

	var pivot btreeKeyRef
	var rightRef btreeNodeRef

	if childRef.isLeaf() {
		childIdx := childRef.index()

		rightIdx, err := m.allocLeaf()
		if err != nil {
			return err
		}

		child := m.leaf(childIdx)
		right := m.leaf(rightIdx)
		n := int(child.n)
		mid := (n + 1) / 2

		right.n = uint16(n - mid)
		copy(right.items[:int(right.n)], child.items[mid:n])

		right.nextLeaf = child.nextLeaf
		child.nextLeaf = rightIdx

		if m.lastLeaf == childIdx {
			m.lastLeaf = rightIdx
		}

		child.n = uint16(mid)

		// Fence key: max key in left child.
		pivot = entryKeyRef(child.items[mid-1])
		rightRef = makeLeafRef(rightIdx)
	} else {
		childIdx := childRef.index()

		rightIdx, err := m.allocInternal()
		if err != nil {
			return err
		}

		parent = m.internal(parentIdx)
		child := m.internal(childIdx)
		right := m.internal(rightIdx)
		n := int(child.n)
		totalChildren := n + 1
		midChild := totalChildren / 2

		pivot = child.keys[midChild-1]

		right.n = uint16(n - midChild)

		copy(right.keys[:int(right.n)], child.keys[midChild:n])
		copy(right.children[:int(right.n)+1], child.children[midChild:n+1])

		child.n = uint16(midChild - 1)
		rightRef = makeInternalRef(rightIdx)
	}

	parent = m.internal(parentIdx)
	pN := int(parent.n)

	copy(parent.keys[childPos+1:pN+1], parent.keys[childPos:pN])
	copy(parent.children[childPos+2:pN+2], parent.children[childPos+1:pN+1])

	parent.keys[childPos] = pivot
	parent.children[childPos+1] = rightRef
	parent.n++

	return nil
}

func (m *btreeMemtable) findLeafLocked(key []byte) uint32 {
	nodeRef := m.root

	for {
		if nodeRef.isLeaf() {
			return nodeRef.index()
		}

		node := m.internal(nodeRef.index())
		nodeRef = node.children[m.childIndex(node, key)]
	}
}

func (m *btreeMemtable) childIndex(node *btreeInternalNode, key []byte) int {
	lo, hi := 0, int(node.n)

	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		// Internal items are fence keys.
		// Go left when key <= fence.
		if m.cmp(key, m.refKey(node.keys[mid])) <= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}

func (m *btreeMemtable) lowerBoundLeaf(node *btreeLeafNode, key []byte) int {
	lo, hi := 0, int(node.n)

	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		if m.cmp(m.entryKey(node.items[mid]), key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}

func (m *btreeMemtable) isFull(ref btreeNodeRef) bool {
	if ref.isLeaf() {
		return m.leaf(ref.index()).n == btreeLeafMaxItems
	}

	return m.internal(ref.index()).n == btreeInternalMaxItems
}

func (m *btreeMemtable) allocLeaf() (uint32, error) {
	if uint64(m.leafCount) >= uint64(btreeLeafBit) {
		return 0, ErrMemtableFull
	}

	idx := m.leafCount
	if idx%btreeLeafChunkSize == 0 {
		m.leafChunks = append(m.leafChunks, new([btreeLeafChunkSize]btreeLeafNode))
	}
	m.leafCount++

	return idx, nil
}

func (m *btreeMemtable) allocInternal() (uint32, error) {
	if uint64(m.internalCount) >= uint64(btreeLeafBit) {
		return 0, ErrMemtableFull
	}

	idx := m.internalCount
	if idx%btreeInternalChunkSize == 0 {
		m.internalChunks = append(m.internalChunks, new([btreeInternalChunkSize]btreeInternalNode))
	}
	m.internalCount++

	return idx, nil
}

func (m *btreeMemtable) leaf(i uint32) *btreeLeafNode {
	return &m.leafChunks[i/btreeLeafChunkSize][i%btreeLeafChunkSize]
}

func (m *btreeMemtable) internal(i uint32) *btreeInternalNode {
	return &m.internalChunks[i/btreeInternalChunkSize][i%btreeInternalChunkSize]
}

func (m *btreeMemtable) appendEntryLocked(key, value []byte) (btreeKVEntry, error) {
	if err := m.checkArenaCapacityForKV(len(key), len(value)); err != nil {
		return btreeKVEntry{}, err
	}

	keyOff := m.dataLen
	valOff := keyOff + len(key)
	end := valOff + len(value)

	copy(m.data[keyOff:valOff], key)
	copy(m.data[valOff:end], value)
	m.dataLen = end

	return btreeKVEntry{
		keyOff: uint32(keyOff),
		keyLen: uint32(len(key)),
		valOff: uint32(valOff),
		valLen: uint32(len(value)),
	}, nil
}

func (m *btreeMemtable) appendBytesLocked(b []byte) (uint32, uint32, error) {
	if err := m.checkArenaCapacity(len(b)); err != nil {
		return 0, 0, err
	}

	off := m.dataLen
	end := off + len(b)

	copy(m.data[off:end], b)
	m.dataLen = end

	return uint32(off), uint32(len(b)), nil
}

func (m *btreeMemtable) checkArenaCapacityForKV(keyLen, valueLen int) error {
	if keyLen < 0 || valueLen < 0 {
		return ErrMemtableFull
	}

	if keyLen > btreeMaxDataBytes || valueLen > btreeMaxDataBytes {
		return ErrMemtableFull
	}

	need := keyLen + valueLen
	if need < keyLen {
		return ErrMemtableFull
	}

	if need > btreeMaxDataBytes-m.dataLen {
		return ErrMemtableFull
	}

	return nil
}

func (m *btreeMemtable) checkArenaCapacity(n int) error {
	if n < 0 {
		return ErrMemtableFull
	}

	if n > btreeMaxDataBytes-m.dataLen {
		return ErrMemtableFull
	}

	return nil
}

func entryKeyRef(e btreeKVEntry) btreeKeyRef {
	return btreeKeyRef{keyOff: e.keyOff, keyLen: e.keyLen}
}

func (m *btreeMemtable) entryKey(e btreeKVEntry) []byte {
	off := int(e.keyOff)
	return m.data[off : off+int(e.keyLen)]
}

func (m *btreeMemtable) refKey(k btreeKeyRef) []byte {
	off := int(k.keyOff)
	return m.data[off : off+int(k.keyLen)]
}

func (m *btreeMemtable) value(e btreeKVEntry) []byte {
	off := int(e.valOff)
	return m.data[off : off+int(e.valLen)]
}

// ----------------------------------------------------------------------------

type btreeCursor struct {
	m         *btreeMemtable
	start     []byte
	end       []byte
	leafIdx   uint32
	batch     [btreeLeafMaxItems]btreeKVEntry
	n         int
	i         int
	exhausted bool
}

func (m *btreeMemtable) Cursor(start, end []byte) Cursor {
	return &btreeCursor{
		m:     m,
		start: start,
		end:   end,
	}
}

func (c *btreeCursor) Next() (key, value []byte, ok bool) {
	for {
		if c.i < c.n {
			e := c.batch[c.i]
			c.i++
			k := c.m.entryKey(e)
			if c.end != nil && c.m.cmp(k, c.end) > 0 {
				c.exhausted = true
				c.n = 0
				return nil, nil, false
			}
			return k, c.m.value(e), true
		}

		if c.exhausted {
			return nil, nil, false
		}

		nextLeaf, n := c.m.fillIterBatch(c.start, c.leafIdx, &c.batch)
		c.leafIdx = nextLeaf
		c.n = n
		c.i = 0

		if n == 0 {
			c.exhausted = true
			return nil, nil, false
		}
		if nextLeaf == 0 {
			c.exhausted = true
		}
	}
}

var _ Cursor = (*btreeCursor)(nil)
