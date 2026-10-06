package memtable

import (
	"encoding/binary"
	"math/bits"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/yashgorana/quxdb/pkg/core"
)

const (
	// each extra level is taken with probability 1/2^slLevelBits
	slLevelBits = 2
	// 12 levels at p=1/4 cover ~16M nodes
	slMaxHeight = 12

	slSlotChunkSize = 4096
	slMaxKeyLen     = 1<<16 - 1 // records store the key length in two bytes

	// record offset, level 0 link and the average extra links, rounded up
	slNodeBytes = 8 + (4+(1<<slLevelBits)-2)/((1<<slLevelBits)-1)
)

// one writer at a time, readers take no lock: nodes never move and are published with atomic stores.
// node x is slots [x, x+height], the record offset then one link per level.
type slArena struct {
	buf     []byte // records, fixed length
	dataLen int

	// fixed-length directory, a chunk is set before any node in it is published
	slots     []*[slSlotChunkSize]atomic.Uint32
	slotCount uint32

	head   uint32
	height atomic.Int32
}

func newArena(capacity int) *slArena {
	maxNodes := capacity/slNodeBytes + 2
	a := &slArena{
		buf: make([]byte, capacity),
		// 4 slots per node is well above the 2.33 average
		slots: make([]*[slSlotChunkSize]atomic.Uint32, (4*maxNodes+slMaxHeight)/slSlotChunkSize+2),
	}
	a.slotCount = 1 // slot 0 is the nil node
	a.head, _ = a.allocNode(slMaxHeight)
	a.height.Store(1)
	return a
}

// allocNode fails only when the slot directory runs out, a node never spans chunks
func (a *slArena) allocNode(height int) (uint32, bool) {
	size := uint32(height + 1)
	x := a.slotCount
	if x%slSlotChunkSize+size > slSlotChunkSize {
		x += slSlotChunkSize - x%slSlotChunkSize
	}
	chunk := int(x / slSlotChunkSize)
	if chunk >= len(a.slots) {
		return 0, false
	}
	if a.slots[chunk] == nil {
		a.slots[chunk] = new([slSlotChunkSize]atomic.Uint32)
	}
	a.slotCount = x + size
	return x, true
}

func (a *slArena) slot(i uint32) *atomic.Uint32 {
	return &a.slots[i/slSlotChunkSize][i%slSlotChunkSize]
}

func (a *slArena) next(x uint32, level int) uint32 {
	return a.slot(x + 1 + uint32(level)).Load()
}

func (a *slArena) setNext(x uint32, level int, next uint32) {
	a.slot(x + 1 + uint32(level)).Store(next)
}

// appendRecord writes a 2-byte keyLen, key, varint valLen, value, Set checks room first
func (a *slArena) appendRecord(key, val []byte) uint32 {
	off := a.dataLen
	b := binary.LittleEndian.AppendUint16(a.buf[off:off], uint16(len(key)))
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(val)))
	b = append(b, val...)
	a.dataLen = off + len(b)
	return uint32(off)
}

func (a *slArena) key(i uint32) []byte {
	b := a.buf[a.slot(i).Load():]
	end := 2 + int(binary.LittleEndian.Uint16(b))
	return b[2:end:end]
}

// value inlines lengths of one or two bytes, values under 16 KiB
func (a *slArena) value(i uint32) []byte {
	b := a.buf[a.slot(i).Load():]
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

type slMemtable struct {
	mu            sync.Mutex // serializes writers and guards the counters below
	arena         atomic.Pointer[slArena]
	cmp           Comparator
	capacityBytes int

	tail       [slMaxHeight]uint32 // last node per level, for ascending inserts
	len        int
	indexBytes int // nodes and links past the head, counted against capacity
	sizeBytes  int
}

func newSkiplistMemtable(opts ...Option) *slMemtable {
	cfg := makeOptions(opts...)
	m := &slMemtable{
		cmp:           cfg.Comparator,
		capacityBytes: cfg.CapacityBytes,
	}
	m.initLocked()
	return m
}

func (m *slMemtable) initLocked() *slArena {
	a := newArena(m.capacityBytes)
	for i := range m.tail {
		m.tail[i] = a.head
	}
	m.len = 0
	m.indexBytes = 0
	m.sizeBytes = 0
	m.arena.Store(a)
	return a
}

func (m *slMemtable) Get(key []byte) ([]byte, bool) {
	a := m.arena.Load()
	if a == nil {
		return nil, false
	}

	x := m.findGreaterOrEqual(a, key, nil)
	if x == 0 || m.cmp(a.key(x), key) != 0 {
		return nil, false
	}
	return a.value(x), true
}

func (m *slMemtable) Seek(key []byte) ([]byte, []byte, bool) {
	a := m.arena.Load()
	if a == nil {
		return nil, nil, false
	}

	x := m.findGreaterOrEqual(a, key, nil)
	if x == 0 {
		return nil, nil, false
	}
	return a.key(x), a.value(x), true
}

func (m *slMemtable) Set(key, val []byte) error {
	if len(key) > slMaxKeyLen {
		return ErrKeyTooLarge
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.arena.Load()
	if a == nil {
		if slPairBytes(len(key), len(val)) > m.capacityBytes {
			return ErrMemtableFull
		}
		a = m.initLocked()
	}

	var prev [slMaxHeight]uint32
	if m.len > 0 && m.cmp(a.key(m.tail[0]), key) < 0 {
		prev = m.tail
	} else {
		x := m.findGreaterOrEqual(a, key, &prev)
		if x != 0 && m.cmp(a.key(x), key) == 0 {
			return m.updateLocked(a, x, key, val)
		}
	}

	if slPairBytes(len(key), len(val)) > m.remaining(a) {
		return ErrMemtableFull
	}
	return m.insertLocked(a, key, val, &prev)
}

// updates append a new record and repoint the node, held values stay intact
func (m *slMemtable) updateLocked(a *slArena, x uint32, key, val []byte) error {
	if slRecordLen(len(key), len(val)) > m.remaining(a) {
		return ErrMemtableFull
	}
	oldLen := len(a.value(x))
	a.slot(x).Store(a.appendRecord(key, val))
	m.sizeBytes += len(val) - oldLen
	return nil
}

// prev holds the predecessor at every level, the new node is linked bottom-up after its own links are set
func (m *slMemtable) insertLocked(a *slArena, key, val []byte, prev *[slMaxHeight]uint32) error {
	h := randomHeight()
	height := int(a.height.Load())
	for i := height; i < h; i++ {
		prev[i] = a.head
	}

	x, ok := a.allocNode(h)
	if !ok {
		return ErrMemtableFull
	}
	a.slot(x).Store(a.appendRecord(key, val))

	for i := range h {
		a.setNext(x, i, a.next(prev[i], i))
	}
	for i := range h {
		a.setNext(prev[i], i, x)
		if a.next(x, i) == 0 {
			m.tail[i] = x
		}
	}
	if h > height {
		a.height.Store(int32(h))
	}

	m.len++
	m.indexBytes += slNodeBytes
	m.sizeBytes += len(key) + len(val)
	return nil
}

func (m *slMemtable) SizeBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sizeBytes
}

func (m *slMemtable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.len
}

func (m *slMemtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.arena.Store(nil)
	m.tail = [slMaxHeight]uint32{}
	m.len = 0
	m.indexBytes = 0
	m.sizeBytes = 0
}

// slRecordLen returns the data bytes one record takes.
func slRecordLen(keyLen, valueLen int) int {
	return 2 + keyLen + (bits.Len64(uint64(valueLen)|1)+6)/7 + valueLen
}

// slPairBytes returns the capacity one key-value pair takes.
func slPairBytes(keyLen, valueLen int) int {
	return slRecordLen(keyLen, valueLen) + slNodeBytes
}

// capacity left for records and nodes
func (m *slMemtable) remaining(a *slArena) int {
	return m.capacityBytes - a.dataLen - m.indexBytes
}

// each trailing pair of zero bits adds a level
func randomHeight() int {
	return min(bits.TrailingZeros32(rand.Uint32())/slLevelBits+1, slMaxHeight)
}

// a node already found >= key is not compared again on lower levels, and it is the result:
// reloading x's successor could return a smaller key a writer just inserted
func (m *slMemtable) findGreaterOrEqual(a *slArena, key []byte, prev *[slMaxHeight]uint32) uint32 {
	x := a.head
	var found uint32
	for level := int(a.height.Load()) - 1; level >= 0; level-- {
		for {
			next := a.next(x, level)
			if next == 0 || next == found {
				break
			}
			if m.cmp(a.key(next), key) >= 0 {
				found = next
				break
			}
			x = next
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return found
}

var _ Memtable = (*slMemtable)(nil)

type slIterator struct {
	a    *slArena
	cmp  Comparator
	end  []byte
	next uint32 // node to return next, 0 once exhausted
}

func (c *slIterator) Next() (key, value []byte, ok bool) {
	if c.next == 0 {
		return nil, nil, false
	}

	key = c.a.key(c.next)
	if c.end != nil && c.cmp(key, c.end) > 0 {
		c.next = 0
		return nil, nil, false
	}
	value = c.a.value(c.next)
	c.next = c.a.next(c.next, 0)
	return key, value, true
}

func (c *slIterator) Err() error {
	return nil
}

func (m *slMemtable) Iterator(start, end []byte) core.Iterator {
	a := m.arena.Load()
	c := &slIterator{a: a, cmp: m.cmp, end: end}
	switch {
	case a == nil:
	case start == nil:
		c.next = a.next(a.head, 0)
	default:
		c.next = m.findGreaterOrEqual(a, start, nil)
	}
	return c
}

var _ core.Iterator = (*slIterator)(nil)
