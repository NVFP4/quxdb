package memtable

import (
	"sync"
)

const (
	btreeLeafMaxItems      = 63
	btreeInternalMaxItems  = 63
	btreeLeafChunkSize     = 128
	btreeInternalChunkSize = 32
	btreeMaxDataBytes      = 16 << 20 // 16 MiB
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

	data      []byte
	dataLen   int
	sizeBytes int
	count     int
}

func newBTreeMemtable(opts ...Option) Memtable {
	cfg := makeOptions(opts...)
	m := &btreeMemtable{cmp: cfg.Comparator}
	m.initLocked()
	return m
}

func (m *btreeMemtable) initLocked() {
	if m.root != 0 {
		return
	}

	m.leafChunks = make([]*[btreeLeafChunkSize]btreeLeafNode, 0, 1)
	m.leafChunks = append(m.leafChunks, new([btreeLeafChunkSize]btreeLeafNode))
	m.internalChunks = nil
	m.leafCount = 2     // leaf 0 = nil, leaf 1 = root
	m.internalCount = 1 // internal 0 = nil; allocate the first chunk on demand
	m.root = makeLeafRef(1)
	m.firstLeaf = 1
	m.lastLeaf = 1
	m.data = make([]byte, btreeMaxDataBytes)
	m.dataLen = 0
	m.sizeBytes = 0
	m.count = 0
}

func (m *btreeMemtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.root == 0 {
		return nil, false
	}

	leafIdx := m.findLeafLocked(key)
	leaf := m.leaf(leafIdx)

	pos := m.lowerBoundLeaf(leaf, key)
	if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
		return m.value(leaf.items[pos]), true
	}

	return nil, false
}

func (m *btreeMemtable) Seek(key []byte) ([]byte, []byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.root == 0 {
		return nil, nil, false
	}

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
	if m.root == 0 && (len(value) > btreeMaxDataBytes || len(key) > btreeMaxDataBytes-len(value)) {
		return ErrMemtableFull
	}
	m.initLocked()

	// An update only needs value bytes.
	if err := m.checkArenaCapacity(len(value)); err != nil {
		return err
	}

	insertFits := m.checkArenaCapacityForKV(len(key), len(value)) == nil
	appendRight := false

	// Right-edge fast path.
	if m.count > 0 && m.lastLeaf != 0 {
		leaf := m.leaf(m.lastLeaf)

		if leaf.n > 0 {
			last := leaf.items[leaf.n-1]
			c := m.cmp(key, m.entryKey(last))

			if c == 0 {
				m.updateValueLocked(m.lastLeaf, int(leaf.n)-1, value)
				return nil
			}

			if c > 0 {
				appendRight = true
				if leaf.n < btreeLeafMaxItems {
					if !insertFits {
						return ErrMemtableFull
					}

					e := m.appendEntryLocked(key, value)
					leaf = m.leaf(m.lastLeaf)
					leaf.items[leaf.n] = e
					leaf.n++
					m.count++
					return nil
				}
			}
		}
	}

	// If a new insert cannot fit, we can still update an existing key.
	// Do a single lookup and only allow update.
	if !insertFits {
		if m.updateExistingInSubtreeLocked(m.root, key, value) {
			return nil
		}

		return ErrMemtableFull
	}

	// Only look ahead for an update when a split is imminent. This keeps the
	// common unique-key path to one traversal while ensuring updates never grow
	// the tree.
	if m.isFull(m.root) {
		if m.updateExistingInSubtreeLocked(m.root, key, value) {
			return nil
		}
		if err := m.splitRootLocked(appendRight); err != nil {
			return err
		}
	}

	nodeRef := m.root

	for {
		if nodeRef.isLeaf() {
			m.insertIntoLeaf(nodeRef.index(), key, value)
			return nil
		}

		nodeIdx := nodeRef.index()
		node := m.internal(nodeIdx)
		childPos := m.childIndex(node, key)
		childRef := node.children[childPos]

		if m.isFull(childRef) {
			if m.updateExistingInSubtreeLocked(childRef, key, value) {
				return nil
			}
			if err := m.splitChild(nodeIdx, childPos, appendRight); err != nil {
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

func (m *btreeMemtable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sizeBytes
}

func (m *btreeMemtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.count
}

func (m *btreeMemtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.root = 0
	m.firstLeaf = 0
	m.lastLeaf = 0
	m.leafCount = 0
	m.internalCount = 0
	m.dataLen = 0
	m.sizeBytes = 0
	m.count = 0
	m.leafChunks = nil
	m.internalChunks = nil
	m.data = nil
}

func (m *btreeMemtable) fillCursorBatch(
	start []byte,
	leafIdx uint32,
	batch *[btreeLeafMaxItems]btreeKVEntry,
) (nextLeaf uint32, n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.root == 0 {
		return 0, 0
	}

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

func (m *btreeMemtable) updateExistingInSubtreeLocked(
	nodeRef btreeNodeRef,
	key, value []byte,
) bool {
	for !nodeRef.isLeaf() {
		node := m.internal(nodeRef.index())
		nodeRef = node.children[m.childIndex(node, key)]
	}

	leafIdx := nodeRef.index()
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)
	if pos >= int(leaf.n) || m.cmp(m.entryKey(leaf.items[pos]), key) != 0 {
		return false
	}

	m.updateValueLocked(leafIdx, pos, value)
	return true
}

func (m *btreeMemtable) updateValueLocked(leafIdx uint32, pos int, value []byte) {
	leaf := m.leaf(leafIdx)
	oldLen := int(leaf.items[pos].valLen)
	off, ln := m.appendBytesLocked(value)
	leaf.items[pos].valOff = off
	leaf.items[pos].valLen = ln
	m.sizeBytes += len(value) - oldLen
}

func (m *btreeMemtable) insertIntoLeaf(leafIdx uint32, key, value []byte) {
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)

	if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
		m.updateValueLocked(leafIdx, pos, value)
		return
	}

	e := m.appendEntryLocked(key, value)
	leaf = m.leaf(leafIdx)

	copy(leaf.items[pos+1:int(leaf.n)+1], leaf.items[pos:int(leaf.n)])
	leaf.items[pos] = e
	leaf.n++
	m.count++
}

func (m *btreeMemtable) splitRootLocked(appendRight bool) error {
	oldRoot := m.root
	leafNodes := uint32(0)
	internalNodes := uint32(1) // the new root
	if oldRoot.isLeaf() {
		leafNodes = 1 // the right sibling
	} else {
		internalNodes++ // the right sibling
	}

	// Preflight every logical allocation before changing topology. A Go heap
	// OOM remains process-fatal, but ErrMemtableFull cannot leave a half-split
	// root behind.
	if !canAllocateNodes(m.leafCount, leafNodes) ||
		!canAllocateNodes(m.internalCount, internalNodes) {
		return ErrMemtableFull
	}

	newRoot, err := m.allocInternal()
	if err != nil {
		panic("memtable: root allocation failed after capacity preflight")
	}

	var rightRef btreeNodeRef
	if oldRoot.isLeaf() {
		rightIdx, err := m.allocLeaf()
		if err != nil {
			panic("memtable: leaf allocation failed after capacity preflight")
		}
		rightRef = makeLeafRef(rightIdx)
	} else {
		rightIdx, err := m.allocInternal()
		if err != nil {
			panic("memtable: internal allocation failed after capacity preflight")
		}
		rightRef = makeInternalRef(rightIdx)
	}

	m.internal(newRoot).children[0] = oldRoot
	m.splitChildWithRight(newRoot, 0, rightRef, appendRight)
	m.root = makeInternalRef(newRoot)
	return nil
}

func (m *btreeMemtable) splitChild(parentIdx uint32, childPos int, appendRight bool) error {
	parent := m.internal(parentIdx)
	childRef := parent.children[childPos]

	var rightRef btreeNodeRef
	if childRef.isLeaf() {
		rightIdx, err := m.allocLeaf()
		if err != nil {
			return err
		}
		rightRef = makeLeafRef(rightIdx)
	} else {
		rightIdx, err := m.allocInternal()
		if err != nil {
			return err
		}
		rightRef = makeInternalRef(rightIdx)
	}

	m.splitChildWithRight(parentIdx, childPos, rightRef, appendRight)
	return nil
}

func (m *btreeMemtable) splitChildWithRight(
	parentIdx uint32,
	childPos int,
	rightRef btreeNodeRef,
	appendRight bool,
) {
	parent := m.internal(parentIdx)
	childRef := parent.children[childPos]
	var pivot btreeKeyRef

	if childRef.isLeaf() {
		childIdx := childRef.index()
		rightIdx := rightRef.index()
		child := m.leaf(childIdx)
		right := m.leaf(rightIdx)
		n := int(child.n)
		mid := (n + 1) / 2
		if appendRight && childIdx == m.lastLeaf {
			// Ascending inserts do not normally revisit this leaf. Keep it full
			// and insert into a new empty right sibling.
			mid = n
		}

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
	} else {
		childIdx := childRef.index()
		rightIdx := rightRef.index()
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
	}

	parent = m.internal(parentIdx)
	pN := int(parent.n)

	copy(parent.keys[childPos+1:pN+1], parent.keys[childPos:pN])
	copy(parent.children[childPos+2:pN+2], parent.children[childPos+1:pN+1])

	parent.keys[childPos] = pivot
	parent.children[childPos+1] = rightRef
	parent.n++
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

func canAllocateNodes(count, n uint32) bool {
	return uint64(count)+uint64(n) <= uint64(btreeLeafBit)
}

func (m *btreeMemtable) allocLeaf() (uint32, error) {
	if !canAllocateNodes(m.leafCount, 1) {
		return 0, ErrMemtableFull
	}

	idx := m.leafCount
	chunkIdx := int(idx / btreeLeafChunkSize)
	if chunkIdx == len(m.leafChunks) {
		m.leafChunks = append(m.leafChunks, new([btreeLeafChunkSize]btreeLeafNode))
	}
	m.leafCount++

	return idx, nil
}

func (m *btreeMemtable) allocInternal() (uint32, error) {
	if !canAllocateNodes(m.internalCount, 1) {
		return 0, ErrMemtableFull
	}

	idx := m.internalCount
	chunkIdx := int(idx / btreeInternalChunkSize)
	if chunkIdx == len(m.internalChunks) {
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

// appendEntryLocked requires Set to have preflighted key/value capacity.
func (m *btreeMemtable) appendEntryLocked(key, value []byte) btreeKVEntry {
	keyOff := m.dataLen
	valOff := keyOff + len(key)
	end := valOff + len(value)

	copy(m.data[keyOff:valOff], key)
	copy(m.data[valOff:end], value)
	m.dataLen = end
	m.sizeBytes += len(key) + len(value)

	return btreeKVEntry{
		keyOff: uint32(keyOff),
		keyLen: uint32(len(key)),
		valOff: uint32(valOff),
		valLen: uint32(len(value)),
	}
}

// appendBytesLocked requires Set to have preflighted value capacity.
func (m *btreeMemtable) appendBytesLocked(b []byte) (uint32, uint32) {
	off := m.dataLen
	end := off + len(b)

	copy(m.data[off:end], b)
	m.dataLen = end

	return uint32(off), uint32(len(b))
}

func (m *btreeMemtable) checkArenaCapacityForKV(keyLen, valueLen int) error {
	remaining := btreeMaxDataBytes - m.dataLen
	if keyLen > remaining || valueLen > remaining-keyLen {
		return ErrMemtableFull
	}

	return nil
}

func (m *btreeMemtable) checkArenaCapacity(n int) error {
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
	end := off + int(e.keyLen)
	return m.data[off:end:end]
}

func (m *btreeMemtable) refKey(k btreeKeyRef) []byte {
	off := int(k.keyOff)
	end := off + int(k.keyLen)
	return m.data[off:end:end]
}

func (m *btreeMemtable) value(e btreeKVEntry) []byte {
	off := int(e.valOff)
	end := off + int(e.valLen)
	return m.data[off:end:end]
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
		start: start[:len(start):len(start)],
		end:   end[:len(end):len(end)],
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

		nextLeaf, n := c.m.fillCursorBatch(c.start, c.leafIdx, &c.batch)
		c.leafIdx = nextLeaf
		c.n = n
		c.i = 0

		if n == 0 {
			if nextLeaf == 0 {
				c.exhausted = true
				return nil, nil, false
			}
			continue
		}
		if nextLeaf == 0 {
			c.exhausted = true
		}
	}
}

var _ Cursor = (*btreeCursor)(nil)
