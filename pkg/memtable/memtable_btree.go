package memtable

import (
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
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

// btreeKVEntry is a record offset in data.
type btreeKVEntry uint32

// readers take no lock: one writer at a time makes a node's version odd while it moves items,
// and readers retry when a version changed under them. appends and value updates keep versions.
type btreeLeafNode struct {
	version  atomic.Uint32
	n        atomic.Uint32
	nextLeaf atomic.Uint32
	items    [btreeLeafMaxItems]atomic.Uint32 // btreeKVEntry
}

type btreeInternalNode struct {
	version atomic.Uint32
	n       atomic.Uint32
	// keys[i] is the max key under children[i]
	keys     [btreeInternalMaxItems]atomic.Uint32     // btreeKVEntry
	children [btreeInternalMaxItems + 1]atomic.Uint32 // btreeNodeRef
}

type btreeMemtable struct {
	mu            sync.Mutex // serializes writers and guards the counters below
	cmp           Comparator
	capacityBytes int
	root          atomic.Uint32 // btreeNodeRef, 0 until the first Set
	firstLeaf     uint32
	lastLeaf      uint32

	// fixed-length directories, a chunk is set before any node in it is published, index 0 is nil
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
	if m.root.Load() != 0 {
		return
	}

	// every chunk past the first leaf chunk is charged against capacity
	m.leafChunks = make([]*[btreeLeafChunkSize]btreeLeafNode, m.capacityBytes/btreeLeafChunkBytes+2)
	m.leafChunks[0] = new([btreeLeafChunkSize]btreeLeafNode)
	m.internalChunks = make([]*[btreeInternalChunkSize]btreeInternalNode, m.capacityBytes/btreeInternalChunkBytes+2)
	m.leafCount = 2     // leaf 0 is nil, leaf 1 is the root
	m.internalCount = 1 // internal 0 is nil, its chunk allocates on demand
	m.firstLeaf = 1
	m.lastLeaf = 1
	m.data = make([]byte, m.capacityBytes)
	m.dataLen = 0
	m.indexBytes = 0
	m.sizeBytes = 0
	m.count = 0
	m.root.Store(uint32(makeLeafRef(1)))
}

func (m *btreeMemtable) Get(key []byte) ([]byte, bool) {
	if m.root.Load() == 0 {
		return nil, false
	}
	for {
		leafIdx, v := m.descend(key)
		leaf := m.leaf(leafIdx)
		pos := m.lowerBoundLeaf(leaf, key)
		hit := false
		var e btreeKVEntry
		if pos < int(leaf.n.Load()) {
			e = btreeKVEntry(leaf.items[pos].Load())
			hit = m.cmp(m.entryKey(e), key) == 0
		}
		if leaf.version.Load() != v {
			continue
		}
		if hit {
			return m.value(e), true
		}
		return nil, false
	}
}

func (m *btreeMemtable) Seek(key []byte) ([]byte, []byte, bool) {
	if m.root.Load() == 0 {
		return nil, nil, false
	}
	for {
		leafIdx, v := m.descend(key)
		if e, found, ok := m.seekFrom(leafIdx, v, m.lowerBoundLeaf(m.leaf(leafIdx), key)); ok {
			if !found {
				return nil, nil, false
			}
			return m.entryKey(e), m.value(e), true
		}
	}
}

// seekFrom returns the first entry at or after pos, walking right, ok is false when a leaf changed
func (m *btreeMemtable) seekFrom(leafIdx, v uint32, pos int) (e btreeKVEntry, found, ok bool) {
	for {
		leaf := m.leaf(leafIdx)
		if pos < int(leaf.n.Load()) {
			e = btreeKVEntry(leaf.items[pos].Load())
			return e, true, leaf.version.Load() == v
		}
		next := leaf.nextLeaf.Load()
		if leaf.version.Load() != v {
			return 0, false, false
		}
		if next == 0 {
			return 0, false, true
		}
		leafIdx, v, pos = next, stableVersion(&m.leaf(next).version), 0
	}
}

// descend returns key's leaf and the leaf version it was found under
func (m *btreeMemtable) descend(key []byte) (leafIdx, version uint32) {
	for {
		if leafIdx, version, ok := m.tryDescend(key); ok {
			return leafIdx, version
		}
	}
}

// each child is entered only after its parent's version is rechecked, so a split under the reader forces a retry
func (m *btreeMemtable) tryDescend(key []byte) (uint32, uint32, bool) {
	ref := btreeNodeRef(m.root.Load())
	v := stableVersion(m.versionOf(ref))
	if btreeNodeRef(m.root.Load()) != ref {
		return 0, 0, false
	}
	for !ref.isLeaf() {
		node := m.internal(ref.index())
		child := btreeNodeRef(node.children[m.childIndex(node, key)].Load())
		cv := stableVersion(m.versionOf(child))
		if node.version.Load() != v {
			return 0, 0, false
		}
		ref, v = child, cv
	}
	return ref.index(), v, true
}

func (m *btreeMemtable) versionOf(ref btreeNodeRef) *atomic.Uint32 {
	if ref.isLeaf() {
		return &m.leaf(ref.index()).version
	}
	return &m.internal(ref.index()).version
}

// stableVersion waits out a writer moving the node's items
func stableVersion(v *atomic.Uint32) uint32 {
	for {
		if x := v.Load(); x&1 == 0 {
			return x
		}
		runtime.Gosched()
	}
}

func (m *btreeMemtable) Set(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(key) > btreeMaxKeyLen {
		return ErrKeyTooLarge
	}
	need := btreeRecordLen(len(key), len(value))
	if m.root.Load() == 0 && need > m.capacityBytes {
		return ErrMemtableFull
	}
	m.initLocked()

	// updates append a full record too
	if need > m.remaining() {
		return ErrMemtableFull
	}
	appendRight := false

	// ascending keys append to the last leaf
	if m.count > 0 {
		leaf := m.leaf(m.lastLeaf)
		n := leaf.n.Load()
		if n > 0 {
			c := m.cmp(key, m.entryKey(btreeKVEntry(leaf.items[n-1].Load())))
			if c == 0 {
				m.updateValueLocked(m.lastLeaf, int(n)-1, key, value)
				return nil
			}

			if c > 0 {
				appendRight = true
				if n < btreeLeafMaxItems {
					// existing items stay put, so readers need no version change
					leaf.items[n].Store(uint32(m.appendEntryLocked(key, value)))
					leaf.n.Store(n + 1)
					m.count++
					return nil
				}
			}
		}
	}

	// look for an update only before a split, so updates never grow the tree
	root := btreeNodeRef(m.root.Load())
	if m.isFull(root) {
		if m.updateExistingInSubtreeLocked(root, key, value) {
			return nil
		}
		if err := m.splitRootLocked(appendRight, need); err != nil {
			return err
		}
	}

	nodeRef := btreeNodeRef(m.root.Load())
	for {
		if nodeRef.isLeaf() {
			m.insertIntoLeaf(nodeRef.index(), key, value)
			return nil
		}

		nodeIdx := nodeRef.index()
		node := m.internal(nodeIdx)
		childPos := m.childIndex(node, key)
		childRef := btreeNodeRef(node.children[childPos].Load())

		if m.isFull(childRef) {
			if m.updateExistingInSubtreeLocked(childRef, key, value) {
				return nil
			}
			if err := m.splitChild(nodeIdx, childPos, appendRight, need); err != nil {
				return err
			}

			if m.cmp(key, m.entryKey(btreeKVEntry(node.keys[childPos].Load()))) > 0 {
				childPos++
			}
			childRef = btreeNodeRef(node.children[childPos].Load())
		}

		nodeRef = childRef
	}
}

func (m *btreeMemtable) SizeBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.sizeBytes
}

func (m *btreeMemtable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.count
}

func (m *btreeMemtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.root.Store(0)
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
	if m.root.Load() == 0 {
		return 0, 0
	}
	for {
		leafIdx, v, pos := m.firstLeaf, stableVersion(&m.leaf(m.firstLeaf).version), 0
		if start != nil {
			leafIdx, v = m.descend(start)
			pos = m.lowerBoundLeaf(m.leaf(leafIdx), start)
		}
		if nextLeaf, n, ok := m.copyLeaf(leafIdx, v, pos, batch); ok {
			return nextLeaf, n
		}
	}
}

func (m *btreeMemtable) fillNextBatch(leafIdx uint32, batch *[btreeLeafMaxItems]btreeKVEntry) (nextLeaf uint32, n int) {
	for {
		v := stableVersion(&m.leaf(leafIdx).version)
		if nextLeaf, n, ok := m.copyLeaf(leafIdx, v, 0, batch); ok {
			return nextLeaf, n
		}
	}
}

// copyLeaf copies items from pos, ok is false when the leaf changed during the copy
func (m *btreeMemtable) copyLeaf(leafIdx, v uint32, pos int, batch *[btreeLeafMaxItems]btreeKVEntry) (nextLeaf uint32, n int, ok bool) {
	leaf := m.leaf(leafIdx)
	for i := pos; i < int(leaf.n.Load()); i++ {
		batch[n] = btreeKVEntry(leaf.items[i].Load())
		n++
	}
	nextLeaf = leaf.nextLeaf.Load()
	return nextLeaf, n, leaf.version.Load() == v
}

func (m *btreeMemtable) updateExistingInSubtreeLocked(
	nodeRef btreeNodeRef,
	key, value []byte,
) bool {
	for !nodeRef.isLeaf() {
		node := m.internal(nodeRef.index())
		nodeRef = btreeNodeRef(node.children[m.childIndex(node, key)].Load())
	}

	leafIdx := nodeRef.index()
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)
	if pos >= int(leaf.n.Load()) || m.cmp(m.entryKey(btreeKVEntry(leaf.items[pos].Load())), key) != 0 {
		return false
	}

	m.updateValueLocked(leafIdx, pos, key, value)
	return true
}

// records are immutable, updates append a new one and swap the item in one store
func (m *btreeMemtable) updateValueLocked(leafIdx uint32, pos int, key, value []byte) {
	item := &m.leaf(leafIdx).items[pos]
	oldLen := len(m.value(btreeKVEntry(item.Load())))
	item.Store(uint32(m.appendEntryLocked(key, value)))
	// appendEntryLocked counted the new pair, drop the replaced one
	m.sizeBytes -= len(key) + oldLen
}

func (m *btreeMemtable) insertIntoLeaf(leafIdx uint32, key, value []byte) {
	leaf := m.leaf(leafIdx)
	pos := m.lowerBoundLeaf(leaf, key)
	n := int(leaf.n.Load())

	if pos < n && m.cmp(m.entryKey(btreeKVEntry(leaf.items[pos].Load())), key) == 0 {
		m.updateValueLocked(leafIdx, pos, key, value)
		return
	}

	e := m.appendEntryLocked(key, value)
	beginWrite(&leaf.version)
	for i := n; i > pos; i-- {
		leaf.items[i].Store(leaf.items[i-1].Load())
	}
	leaf.items[pos].Store(uint32(e))
	leaf.n.Store(uint32(n + 1))
	endWrite(&leaf.version)
	m.count++
}

// beginWrite and endWrite bracket item moves, an odd version tells readers to wait and retry
func beginWrite(v *atomic.Uint32) { v.Add(1) }
func endWrite(v *atomic.Uint32)   { v.Add(1) }

// new node chunks must leave reserve bytes for the pending record
func (m *btreeMemtable) splitRootLocked(appendRight bool, reserve int) error {
	oldRoot := btreeNodeRef(m.root.Load())
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

	// the old root stays odd until the new root is published, so readers that entered it retry
	old := m.versionOf(oldRoot)
	beginWrite(old)
	m.internal(newRoot).children[0].Store(uint32(oldRoot))
	m.splitChildWithRight(newRoot, 0, rightRef, appendRight)
	m.root.Store(uint32(makeInternalRef(newRoot)))
	endWrite(old)
	return nil
}

func (m *btreeMemtable) splitChild(parentIdx uint32, childPos int, appendRight bool, reserve int) error {
	parent := m.internal(parentIdx)
	childRef := btreeNodeRef(parent.children[childPos].Load())

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

	child := m.versionOf(childRef)
	beginWrite(&parent.version)
	beginWrite(child)
	m.splitChildWithRight(parentIdx, childPos, rightRef, appendRight)
	endWrite(child)
	endWrite(&parent.version)
	return nil
}

// fills the unpublished right node first, callers keep the parent and child versions odd
func (m *btreeMemtable) splitChildWithRight(
	parentIdx uint32,
	childPos int,
	rightRef btreeNodeRef,
	appendRight bool,
) {
	parent := m.internal(parentIdx)
	childRef := btreeNodeRef(parent.children[childPos].Load())
	var leftMax uint32

	if childRef.isLeaf() {
		childIdx := childRef.index()
		rightIdx := rightRef.index()
		child := m.leaf(childIdx)
		right := m.leaf(rightIdx)
		n := int(child.n.Load())
		mid := (n + 1) / 2
		if appendRight && childIdx == m.lastLeaf {
			// ascending inserts won't revisit this leaf, keep it full
			mid = n
		}

		for i := mid; i < n; i++ {
			right.items[i-mid].Store(child.items[i].Load())
		}
		right.n.Store(uint32(n - mid))
		right.nextLeaf.Store(child.nextLeaf.Load())
		child.nextLeaf.Store(rightIdx)
		if m.lastLeaf == childIdx {
			m.lastLeaf = rightIdx
		}
		child.n.Store(uint32(mid))

		leftMax = child.items[mid-1].Load()
	} else {
		child := m.internal(childRef.index())
		right := m.internal(rightRef.index())
		n := int(child.n.Load())
		midChild := (n + 1) / 2

		leftMax = child.keys[midChild-1].Load()
		for i := midChild; i < n; i++ {
			right.keys[i-midChild].Store(child.keys[i].Load())
		}
		for i := midChild; i <= n; i++ {
			right.children[i-midChild].Store(child.children[i].Load())
		}
		right.n.Store(uint32(n - midChild))
		child.n.Store(uint32(midChild - 1))
	}

	pN := int(parent.n.Load())
	for i := pN; i > childPos; i-- {
		parent.keys[i].Store(parent.keys[i-1].Load())
		parent.children[i+1].Store(parent.children[i].Load())
	}
	parent.keys[childPos].Store(leftMax)
	parent.children[childPos+1].Store(uint32(rightRef))
	parent.n.Store(uint32(pN + 1))
}

func (m *btreeMemtable) childIndex(node *btreeInternalNode, key []byte) int {
	lo, hi := 0, int(node.n.Load())

	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		// children[mid] holds keys <= keys[mid]
		if m.cmp(key, m.entryKey(btreeKVEntry(node.keys[mid].Load()))) <= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}

func (m *btreeMemtable) lowerBoundLeaf(node *btreeLeafNode, key []byte) int {
	lo, hi := 0, int(node.n.Load())

	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		if m.cmp(m.entryKey(btreeKVEntry(node.items[mid].Load())), key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return lo
}

func (m *btreeMemtable) isFull(ref btreeNodeRef) bool {
	if ref.isLeaf() {
		return m.leaf(ref.index()).n.Load() == btreeLeafMaxItems
	}

	return m.internal(ref.index()).n.Load() == btreeInternalMaxItems
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
	if chunkIdx >= len(m.leafChunks) {
		return 0, ErrMemtableFull
	}
	if m.leafChunks[chunkIdx] == nil {
		if btreeLeafChunkBytes > m.remaining()-reserve {
			return 0, ErrMemtableFull
		}
		m.leafChunks[chunkIdx] = new([btreeLeafChunkSize]btreeLeafNode)
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
	if chunkIdx >= len(m.internalChunks) {
		return 0, ErrMemtableFull
	}
	if m.internalChunks[chunkIdx] == nil {
		if btreeInternalChunkBytes > m.remaining()-reserve {
			return 0, ErrMemtableFull
		}
		m.internalChunks[chunkIdx] = new([btreeInternalChunkSize]btreeInternalNode)
		m.indexBytes += btreeInternalChunkBytes
	}
	m.internalCount++

	return idx, nil
}

// bytes of the node chunks that allocating these nodes would add
func (m *btreeMemtable) newChunkBytes(leafNodes, internalNodes uint32) int {
	var n int
	if leafNodes > 0 && needsChunk(m.leafChunks, int((m.leafCount+leafNodes-1)/btreeLeafChunkSize)) {
		n += btreeLeafChunkBytes
	}
	if internalNodes > 0 && needsChunk(m.internalChunks, int((m.internalCount+internalNodes-1)/btreeInternalChunkSize)) {
		n += btreeInternalChunkBytes
	}
	return n
}

func needsChunk[C any](dir []*C, i int) bool {
	return i >= len(dir) || dir[i] == nil
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

// appendEntryLocked writes a record: a 2-byte keyLen, key, varint valLen, value.
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
	return 2 + keyLen + (bits.Len64(uint64(valueLen)|1)+6)/7 + valueLen
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
