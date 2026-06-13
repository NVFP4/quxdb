package memtable

import (
	"slices"
	"sync"
	_ "unsafe"
)

const (
	// With p=1/2, height 24 covers roughly 16 million entries at the top
	// level, well above what a 64 MiB key/value data arena should hold.
	slMaxHeight    = 24
	slMaxDataBytes = 1 << 26 // 64 MiB
)

type slNode struct {
	keyOff   uint32
	keyLen   uint32
	valOff   uint32
	valLen   uint32
	next     uint32
	towerOff uint32
	height   uint8
}

type slArena struct {
	nodes []slNode
	links []uint32
	data  []byte
}

func newArena(dataCap int) *slArena {
	a := &slArena{
		nodes: make([]slNode, 0, 2),
		links: make([]uint32, 0, slMaxHeight-1),
		data:  make([]byte, 0, dataCap),
	}

	// node index 0 is reserved as nil.
	a.nodes = append(a.nodes, slNode{})

	return a
}

func (a *slArena) newNode(key, val []byte, height int) uint32 {
	keyOff := uint32(len(a.data))
	a.data = append(a.data, key...)

	valOff := uint32(len(a.data))
	a.data = append(a.data, val...)

	n := slNode{
		keyOff: keyOff,
		keyLen: uint32(len(key)),
		valOff: valOff,
		valLen: uint32(len(val)),
		height: uint8(height),
	}
	if height > 1 {
		n.towerOff = a.allocLinks(height - 1)
	}

	a.nodes = append(a.nodes, n)
	return uint32(len(a.nodes) - 1)
}

func (a *slArena) newHead() uint32 {
	n := slNode{
		height:   slMaxHeight,
		towerOff: a.allocLinks(slMaxHeight - 1),
	}
	a.nodes = append(a.nodes, n)
	return uint32(len(a.nodes) - 1)
}

func (a *slArena) allocLinks(n int) uint32 {
	off := uint32(len(a.links))
	a.links = slices.Grow(a.links, n)[:len(a.links)+n]
	return off
}

func (a *slArena) key(i uint32) []byte {
	n := &a.nodes[i]
	start := int(n.keyOff)
	end := start + int(n.keyLen)
	return a.data[start:end]
}

func (a *slArena) value(i uint32) []byte {
	n := &a.nodes[i]
	start := int(n.valOff)
	end := start + int(n.valLen)
	return a.data[start:end]
}

func (a *slArena) setValue(i uint32, val []byte) error {
	n := &a.nodes[i]
	if len(val) <= int(n.valLen) {
		start := int(n.valOff)
		copy(a.data[start:start+len(val)], val)
		n.valLen = uint32(len(val))
		return nil
	}

	if len(a.data)+len(val) > cap(a.data) {
		return ErrMemtableFull
	}

	valOff := uint32(len(a.data))
	a.data = append(a.data, val...)

	n.valOff = valOff
	n.valLen = uint32(len(val))
	return nil
}

func (a *slArena) next(i uint32, level int) uint32 {
	if level == 0 {
		return a.nodes[i].next
	}
	n := &a.nodes[i]
	return a.links[int(n.towerOff)+level-1]
}

func (a *slArena) setNext(i uint32, level int, next uint32) {
	if level == 0 {
		a.nodes[i].next = next
		return
	}
	n := &a.nodes[i]
	a.links[int(n.towerOff)+level-1] = next
}

type slMemtable struct {
	mu     sync.RWMutex
	arena  *slArena
	cmp    Comparator
	head   uint32
	tail   [slMaxHeight]uint32
	height int
	len    int
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32

func newSkiplistMemtable(opts ...Option) *slMemtable {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	a := newArena(slMaxDataBytes)
	m := &slMemtable{
		arena:  a,
		cmp:    cfg.Comparator,
		head:   a.newHead(),
		height: 1,
	}
	for i := range slMaxHeight {
		m.tail[i] = m.head
	}
	return m
}

func (m *slMemtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	x := m.findGreaterOrEqual(key, nil)
	if x == 0 || m.cmp(m.arena.key(x), key) != 0 {
		return nil, false
	}

	// This aliases arena memory. Caller must not mutate it.
	return m.arena.value(x), true
}

func (m *slMemtable) SeekGE(key []byte) ([]byte, []byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	x := m.findGreaterOrEqual(key, nil)
	if x == 0 {
		return nil, nil, false
	}

	return m.arena.key(x), m.arena.value(x), true
}

func (m *slMemtable) Set(key, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.setLocked(key, val)
}

func (m *slMemtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var prev [slMaxHeight]uint32
	x := m.findGreaterOrEqual(key, &prev)
	if x == 0 || m.cmp(m.arena.key(x), key) != 0 {
		return
	}

	h := int(m.arena.nodes[x].height)
	for i := range h {
		m.arena.setNext(prev[i], i, m.arena.next(x, i))
		if m.tail[i] == x {
			m.tail[i] = prev[i]
		}
	}
	for m.height > 1 && m.arena.next(m.head, m.height-1) == 0 {
		m.height--
	}
	m.len--
}

func (m *slMemtable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.arena.data)
}

func (m *slMemtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.len
}

func (m *slMemtable) Iter() Iterator {
	return m.IterRange(nil, nil)
}

func (m *slMemtable) IterFrom(key []byte) Iterator {
	return m.IterRange(key, nil)
}

func (m *slMemtable) IterRange(start, end []byte) Iterator {
	return func(yield func([]byte, []byte) bool) {
		m.mu.RLock()
		defer m.mu.RUnlock()

		x := m.arena.next(m.head, 0)
		if start != nil {
			x = m.findGreaterOrEqual(start, nil)
		}

		for ; x != 0; x = m.arena.next(x, 0) {
			if end != nil && m.cmp(m.arena.key(x), end) > 0 {
				return
			}
			if !yield(m.arena.key(x), m.arena.value(x)) {
				return
			}
		}
	}
}

func (m *slMemtable) setLocked(key, val []byte) error {
	if m.len > 0 && m.cmp(m.arena.key(m.tail[0]), key) < 0 {
		if len(m.arena.data)+len(key)+len(val) > cap(m.arena.data) {
			return ErrMemtableFull
		}
		m.append(key, val)
		return nil
	}

	var prev [slMaxHeight]uint32
	x := m.findGreaterOrEqual(key, &prev)
	if x != 0 && m.cmp(m.arena.key(x), key) == 0 {
		return m.arena.setValue(x, val)
	}

	if len(m.arena.data)+len(key)+len(val) > cap(m.arena.data) {
		return ErrMemtableFull
	}

	m.insertAfter(key, val, &prev)
	return nil
}

func (m *slMemtable) append(key, val []byte) {
	h := m.randomHeight()
	if h > m.height {
		m.height = h
	}

	n := m.arena.newNode(key, val, h)

	for i := range h {
		m.arena.setNext(m.tail[i], i, n)
		m.tail[i] = n
	}

	m.len++
}

func (m *slMemtable) insertAfter(key, val []byte, prev *[slMaxHeight]uint32) {
	h := m.randomHeight()

	if h > m.height {
		for i := m.height; i < h; i++ {
			prev[i] = m.head
		}
		m.height = h
	}

	n := m.arena.newNode(key, val, h)

	for i := range h {
		next := m.arena.next(prev[i], i)
		m.arena.setNext(n, i, next)
		m.arena.setNext(prev[i], i, n)
		if next == 0 {
			m.tail[i] = n
		}
	}

	m.len++
}

func (m *slMemtable) randomHeight() int {
	x := fastrand()
	h := 1
	// p==0.5
	for h < slMaxHeight && x&1 == 0 {
		h++
		x >>= 1
	}
	return h
}

func (m *slMemtable) findGreaterOrEqual(key []byte, prev *[slMaxHeight]uint32) uint32 {
	x := m.head
	cmp := m.cmp

	for level := m.height - 1; level >= 0; level-- {
		for {
			next := m.arena.next(x, level)
			if next == 0 {
				break
			}

			if cmp(m.arena.key(next), key) < 0 {
				x = next
				continue
			}

			break
		}

		if prev != nil {
			prev[level] = x
		}
	}

	return m.arena.next(x, 0)
}

var _ Memtable = (*slMemtable)(nil)
