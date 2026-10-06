package memtable

import (
	"encoding/binary"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/yashgorana/quxdb/pkg/core"
)

const (
	btreeLeafMaxItems      = 63
	btreeInternalMaxItems  = 63
	btreeLeafChunkSize     = 128
	btreeInternalChunkSize = 32
	btreeLeafBit           = 1 << 31
	btreeMaxKeyLen         = 1<<16 - 1 // records store the key length in two bytes

	btreeLeafChunkBytes     = int(unsafe.Sizeof([btreeLeafChunkSize]btreeLeafNode{}))
	btreeInternalChunkBytes = int(unsafe.Sizeof([btreeInternalChunkSize]btreeInternalNode{}))
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

// btreeKVEntry is a record offset in data, a record is a 2-byte keyLen, key, varint valLen, value.
type btreeKVEntry uint32

type btreeLeafNode struct {
	n        uint16
	nextLeaf uint32

	items [btreeLeafMaxItems]btreeKVEntry
}

type btreeInternalNode struct {
	n uint16

	// keys[i] is the max key under children[i]
	keys     [btreeInternalMaxItems]btreeKVEntry
	children [btreeInternalMaxItems + 1]btreeNodeRef
}

type btreeMemtable struct {
	mu            sync.RWMutex
	cmp           Comparator
	capacityBytes int
	root          btreeNodeRef
	firstLeaf     uint32
	lastLeaf      uint32

	// index 0 is nil in both node arrays
	leafChunks     []*[btreeLeafChunkSize]btreeLeafNode
	leafCount      uint32
	internalChunks []*[btreeInternalChunkSize]btreeInternalNode
	internalCount  uint32

	data       []byte
	dataLen    int
	indexBytes int // node chunks past the first leaf chunk, counted against capacity
	sizeBytes  int
	count      int
}

func newBTreeMemtable(opts ...Option) Memtable {
	cfg := makeOptions(opts...)
	m := &btreeMemtable{
		cmp:           cfg.Comparator,
		capacityBytes: cfg.CapacityBytes,
	}
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
	m.leafCount = 2     // leaf 0 is nil, leaf 1 is the root
	m.internalCount = 1 // internal 0 is nil, its chunk allocates on demand
	m.root = makeLeafRef(1)
	m.firstLeaf = 1
	m.lastLeaf = 1
	m.data = make([]byte, m.capacityBytes)
	m.dataLen = 0
	m.indexBytes = 0
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
	if len(key) > btreeMaxKeyLen {
		return ErrKeyTooLarge
	}
	need := btreeRecordLen(len(key), len(value))
	if m.root == 0 && need > m.capacityBytes {
		return ErrMemtableFull
	}
	m.initLocked()

	// updates append a full record too
	if need > m.remaining() {
		return ErrMemtableFull
	}
	appendRight := false

	// ascending keys append to the last leaf
	if m.count > 0 && m.lastLeaf != 0 {
		leaf := m.leaf(m.lastLeaf)

		if leaf.n > 0 {
			last := leaf.items[leaf.n-1]
			c := m.cmp(key, m.entryKey(last))

			if c == 0 {
				m.updateValueLocked(m.lastLeaf, int(leaf.n)-1, key, value)
				return nil
			}

			if c > 0 {
				appendRight = true
				if leaf.n < btreeLeafMaxItems {
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

	// look for an update only before a split, so updates never grow the tree
	if m.isFull(m.root) {
		if m.updateExistingInSubtreeLocked(m.root, key, value) {
			return nil
		}
		if err := m.splitRootLocked(appendRight, need); err != nil {
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
			if err := m.splitChild(nodeIdx, childPos, appendRight, need); err != nil {
				return err
			}

			node = m.internal(nodeIdx)

			if m.cmp(key, m.entryKey(node.keys[childPos])) > 0 {
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
	m.indexBytes = 0
	m.sizeBytes = 0
	m.count = 0
	m.leafChunks = nil
	m.internalChunks = nil
	m.data = nil
}

// fills batch from the first entry >= start, nil start is the first entry
func (m *btreeMemtable) fillFirstBatch(start []byte, batch *[btreeLeafMaxItems]btreeKVEntry) (nextLeaf uint32, n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.root == 0 {
		return 0, 0
	}
	if start == nil {
		return m.copyLeafLocked(m.firstLeaf, 0, batch)
	}
	leafIdx := m.findLeafLocked(start)
	return m.copyLeafLocked(leafIdx, m.lowerBoundLeaf(m.leaf(leafIdx), start), batch)
}

func (m *btreeMemtable) fillNextBatch(leafIdx uint32, batch *[btreeLeafMaxItems]btreeKVEntry) (nextLeaf uint32, n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.copyLeafLocked(leafIdx, 0, batch)
}

func (m *btreeMemtable) copyLeafLocked(leafIdx uint32, pos int, batch *[btreeLeafMaxItems]btreeKVEntry) (nextLeaf uint32, n int) {
	leaf := m.leaf(leafIdx)
	return leaf.nextLeaf, copy(batch[:], leaf.items[pos:int(leaf.n)])
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

	m.updateValueLocked(leafIdx, pos, key, value)
	return true
}

// records are immutable, updates append a new one
func (m *btreeMemtable) updateValueLocked(leafIdx uint32, pos int, key, value []byte) {
	leaf := m.leaf(leafIdx)
	oldLen := len(m.value(leaf.items[pos]))
	e := m.appendEntryLocked(key, value)
	leaf = m.leaf(leafIdx)
	leaf.items[pos] = e
	// appendEntryLocked counted the new pair, drop the replaced one
	m.sizeBytes -= len(key) + oldLen
}

func (m *btreeMemtable) insertIntoLeaf(leafIdx uint32, key, value []byte) {
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)

	if pos < int(leaf.n) && m.cmp(m.entryKey(leaf.items[pos]), key) == 0 {
		m.updateValueLocked(leafIdx, pos, key, value)
		return
	}

	e := m.appendEntryLocked(key, value)
	leaf = m.leaf(leafIdx)

	copy(leaf.items[pos+1:int(leaf.n)+1], leaf.items[pos:int(leaf.n)])
	leaf.items[pos] = e
	leaf.n++
	m.count++
}

// new node chunks must leave reserve bytes for the pending record
func (m *btreeMemtable) splitRootLocked(appendRight bool, reserve int) error {
	oldRoot := m.root
	leafNodes := uint32(0)
	internalNodes := uint32(1) // the new root
	if oldRoot.isLeaf() {
		leafNodes = 1 // the right sibling
	} else {
		internalNodes++ // the right sibling
	}

	// check room before changing the tree so ErrMemtableFull never leaves a half-split root
	if !canAllocateNodes(m.leafCount, leafNodes) ||
		!canAllocateNodes(m.internalCount, internalNodes) ||
		m.newChunkBytes(leafNodes, internalNodes) > m.remaining()-reserve {
		return ErrMemtableFull
	}

	newRoot, err := m.allocInternal(reserve)
	if err != nil {
		panic("memtable: root allocation failed after capacity preflight")
	}

	var rightRef btreeNodeRef
	if oldRoot.isLeaf() {
		rightIdx, err := m.allocLeaf(reserve)
		if err != nil {
			panic("memtable: leaf allocation failed after capacity preflight")
		}
		rightRef = makeLeafRef(rightIdx)
	} else {
		rightIdx, err := m.allocInternal(reserve)
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

func (m *btreeMemtable) splitChild(parentIdx uint32, childPos int, appendRight bool, reserve int) error {
	parent := m.internal(parentIdx)
	childRef := parent.children[childPos]

	var rightRef btreeNodeRef
	if childRef.isLeaf() {
		rightIdx, err := m.allocLeaf(reserve)
		if err != nil {
			return err
		}
		rightRef = makeLeafRef(rightIdx)
	} else {
		rightIdx, err := m.allocInternal(reserve)
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
	var leftMax btreeKVEntry

	if childRef.isLeaf() {
		childIdx := childRef.index()
		rightIdx := rightRef.index()
		child := m.leaf(childIdx)
		right := m.leaf(rightIdx)
		n := int(child.n)
		mid := (n + 1) / 2
		if appendRight && childIdx == m.lastLeaf {
			// ascending inserts won't revisit this leaf, keep it full
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

		leftMax = child.items[mid-1]
	} else {
		childIdx := childRef.index()
		rightIdx := rightRef.index()
		child := m.internal(childIdx)
		right := m.internal(rightIdx)
		n := int(child.n)
		totalChildren := n + 1
		midChild := totalChildren / 2

		leftMax = child.keys[midChild-1]
		right.n = uint16(n - midChild)
		copy(right.keys[:int(right.n)], child.keys[midChild:n])
		copy(right.children[:int(right.n)+1], child.children[midChild:n+1])
		child.n = uint16(midChild - 1)
	}

	parent = m.internal(parentIdx)
	pN := int(parent.n)

	copy(parent.keys[childPos+1:pN+1], parent.keys[childPos:pN])
	copy(parent.children[childPos+2:pN+2], parent.children[childPos+1:pN+1])

	parent.keys[childPos] = leftMax
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

		// children[mid] holds keys <= keys[mid]
		if m.cmp(key, m.entryKey(node.keys[mid])) <= 0 {
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

func (m *btreeMemtable) allocLeaf(reserve int) (uint32, error) {
	if !canAllocateNodes(m.leafCount, 1) {
		return 0, ErrMemtableFull
	}

	idx := m.leafCount
	chunkIdx := int(idx / btreeLeafChunkSize)
	if chunkIdx == len(m.leafChunks) {
		if btreeLeafChunkBytes > m.remaining()-reserve {
			return 0, ErrMemtableFull
		}
		m.leafChunks = append(m.leafChunks, new([btreeLeafChunkSize]btreeLeafNode))
		m.indexBytes += btreeLeafChunkBytes
	}
	m.leafCount++

	return idx, nil
}

func (m *btreeMemtable) allocInternal(reserve int) (uint32, error) {
	if !canAllocateNodes(m.internalCount, 1) {
		return 0, ErrMemtableFull
	}

	idx := m.internalCount
	chunkIdx := int(idx / btreeInternalChunkSize)
	if chunkIdx == len(m.internalChunks) {
		if btreeInternalChunkBytes > m.remaining()-reserve {
			return 0, ErrMemtableFull
		}
		m.internalChunks = append(m.internalChunks, new([btreeInternalChunkSize]btreeInternalNode))
		m.indexBytes += btreeInternalChunkBytes
	}
	m.internalCount++

	return idx, nil
}

// bytes of the node chunks that allocating these nodes would add
func (m *btreeMemtable) newChunkBytes(leafNodes, internalNodes uint32) int {
	var n int
	if leafNodes > 0 && int((m.leafCount+leafNodes-1)/btreeLeafChunkSize) >= len(m.leafChunks) {
		n += btreeLeafChunkBytes
	}
	if internalNodes > 0 && int((m.internalCount+internalNodes-1)/btreeInternalChunkSize) >= len(m.internalChunks) {
		n += btreeInternalChunkBytes
	}
	return n
}

// capacity left for data and node chunks
func (m *btreeMemtable) remaining() int {
	return m.capacityBytes - m.dataLen - m.indexBytes
}

func (m *btreeMemtable) leaf(i uint32) *btreeLeafNode {
	return &m.leafChunks[i/btreeLeafChunkSize][i%btreeLeafChunkSize]
}

func (m *btreeMemtable) internal(i uint32) *btreeInternalNode {
	return &m.internalChunks[i/btreeInternalChunkSize][i%btreeInternalChunkSize]
}

// appendEntryLocked requires Set to have preflighted the record's capacity.
func (m *btreeMemtable) appendEntryLocked(key, value []byte) btreeKVEntry {
	off := m.dataLen
	b := binary.LittleEndian.AppendUint16(m.data[off:off], uint16(len(key)))
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(value)))
	b = append(b, value...)
	m.dataLen = off + len(b)
	m.sizeBytes += len(key) + len(value)

	return btreeKVEntry(off)
}

// btreeRecordLen returns the data bytes one record takes.
func btreeRecordLen(keyLen, valueLen int) int {
	return 2 + keyLen + uvarintLen(valueLen) + valueLen
}

func uvarintLen(n int) int {
	return (bits.Len64(uint64(n)|1) + 6) / 7
}

func (m *btreeMemtable) entryKey(e btreeKVEntry) []byte {
	b := m.data[e:]
	end := 2 + int(binary.LittleEndian.Uint16(b))
	return b[2:end:end]
}

// value inlines lengths of one or two bytes, values under 16 KiB
func (m *btreeMemtable) value(e btreeKVEntry) []byte {
	b := m.data[e:]
	off := 2 + int(binary.LittleEndian.Uint16(b))
	vl, n := int(b[off]), 1
	if vl >= 0x80 {
		if c := int(b[off+1]); c < 0x80 {
			vl, n = vl&0x7f|c<<7, 2
		} else {
			u, w := binary.Uvarint(b[off:])
			vl, n = int(u), w
		}
	}
	off += n
	return b[off : off+vl : off+vl]
}

// ----------------------------------------------------------------------------

type btreeIterator struct {
	m       *btreeMemtable
	end     []byte
	leafIdx uint32 // next leaf to copy, 0 once the leaves run out
	batch   [btreeLeafMaxItems]btreeKVEntry
	n       int
	i       int
}

func (m *btreeMemtable) Iterator(start, end []byte) core.Iterator {
	c := &btreeIterator{m: m, end: end}
	c.leafIdx, c.n = m.fillFirstBatch(start, &c.batch)
	return c
}

func (c *btreeIterator) Err() error {
	return nil
}

func (c *btreeIterator) Next() (key, value []byte, ok bool) {
	for c.i == c.n {
		if c.leafIdx == 0 {
			return nil, nil, false
		}
		c.leafIdx, c.n = c.m.fillNextBatch(c.leafIdx, &c.batch)
		c.i = 0
	}

	e := c.batch[c.i]
	c.i++
	k := c.m.entryKey(e)
	if c.end != nil && c.m.cmp(k, c.end) > 0 {
		c.leafIdx, c.n, c.i = 0, 0, 0
		return nil, nil, false
	}
	return k, c.m.value(e), true
}

var _ core.Iterator = (*btreeIterator)(nil)
