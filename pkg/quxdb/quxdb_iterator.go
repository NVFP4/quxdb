package quxdb

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

// mergeIterator k-way merges sources with a loser tree, ties go to the earlier source.
type mergeIterator struct {
	sources []mergeSource
	tree    []int // [0] is the winner, the rest hold losers
	err     error
	advance bool // the winner moves on the next call
}

type mergeSource struct {
	cur   core.Iterator
	key   []byte
	value []byte
	done  bool
}

func newMergeIterator(sources []core.Iterator) *mergeIterator {
	k := len(sources)
	m := &mergeIterator{sources: make([]mergeSource, k), tree: make([]int, max(k, 1))}
	for i, cur := range sources {
		m.sources[i].cur = cur
		if !m.sources[i].next() {
			if m.err = cur.Err(); m.err != nil {
				return m
			}
		}
	}

	// -1 placeholders beat every source until pushed out
	for i := range m.tree {
		m.tree[i] = -1
	}
	for i := k - 1; i >= 0; i-- {
		m.replay(i)
	}
	return m
}

// entries stay valid until the next call.
func (m *mergeIterator) Next() (key, value []byte, ok bool) {
	if m.err != nil || len(m.sources) == 0 {
		return nil, nil, false
	}
	if m.advance {
		m.advance = false
		w := m.tree[0]
		if !m.sources[w].next() {
			if m.err = m.sources[w].cur.Err(); m.err != nil {
				return nil, nil, false
			}
		}
		m.replay(w)
	}

	winner := &m.sources[m.tree[0]]
	if winner.done {
		return nil, nil, false
	}
	m.advance = true
	return winner.key, winner.value, true
}

func (m *mergeIterator) Err() error {
	return m.err
}

func (s *mergeSource) next() bool {
	key, value, ok := s.cur.Next()
	s.key, s.value, s.done = key, value, !ok
	return ok
}

// walks w's leaf to the root, leaving losers behind.
func (m *mergeIterator) replay(w int) {
	k := len(m.sources)
	for node := (w + k) / 2; node > 0; node /= 2 {
		if loser := m.tree[node]; w != -1 && (loser == -1 || m.beats(loser, w)) {
			m.tree[node], w = w, loser
		}
	}
	m.tree[0] = w
}

// exhausted sources lose, ties go to the earlier source.
func (m *mergeIterator) beats(a, b int) bool {
	sa, sb := &m.sources[a], &m.sources[b]
	if sa.done || sb.done {
		return !sa.done || sb.done && a < b
	}
	cmp := bytes.Compare(sa.key, sb.key)
	return cmp < 0 || cmp == 0 && a < b
}

// snapshotIterator yields each user key's newest version at or below readSeq from an iterator sorted by quxKey.
type snapshotIterator struct {
	cur            core.Iterator
	readSeq        quxSeq
	dropTombstones bool

	lastUserKey []byte // nil until the first key, UserKey never returns nil
}

func newSnapshotIterator(cur core.Iterator, readSeq quxSeq, dropTombstones bool) *snapshotIterator {
	return &snapshotIterator{
		cur:            cur,
		readSeq:        readSeq,
		dropTombstones: dropTombstones,
	}
}

func (c *snapshotIterator) Next() (key, value []byte, ok bool) {
	for {
		key, value, ok = c.cur.Next()
		if !ok {
			return nil, nil, false
		}

		qkey := quxKey(key)
		if qkey.Seq() > c.readSeq {
			continue
		}
		userKey := qkey.UserKey()
		// versions sort newest first, so the first visible one wins
		if c.lastUserKey != nil && bytes.Equal(userKey, c.lastUserKey) {
			continue
		}
		c.lastUserKey = userKey

		if qkey.Op() == quxOpDelete && c.dropTombstones {
			continue
		}
		return key, value, true
	}
}

func (c *snapshotIterator) Err() error {
	return c.cur.Err()
}
