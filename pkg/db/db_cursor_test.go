package db

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yashgorana/quxdb/pkg/core"
)

type entry struct {
	key   quxKey
	value string
}

// sliceCursor yields entries in order, then fails with err if set.
type sliceCursor struct {
	entries []entry
	err     error
	pos     int
}

func (c *sliceCursor) Next() ([]byte, []byte, bool) {
	if c.pos >= len(c.entries) {
		return nil, nil, false
	}
	e := c.entries[c.pos]
	c.pos++
	return e.key, []byte(e.value), true
}

func (c *sliceCursor) Err() error {
	if c.pos >= len(c.entries) {
		return c.err
	}
	return nil
}

func set(key string, seq quxSeq, value string) entry {
	return entry{newQuxKey([]byte(key), seq, quxOpSet), value}
}

func del(key string, seq quxSeq) entry {
	return entry{newQuxKey([]byte(key), seq, quxOpDelete), ""}
}

func drain(t *testing.T, cur core.Cursor) []string {
	t.Helper()
	var out []string
	for {
		key, value, ok := cur.Next()
		if !ok {
			require.NoError(t, cur.Err())
			return out
		}
		userKey, seq, op := quxKey(key).Decode()
		if op == quxOpDelete {
			value = []byte("deleted")
		}
		out = append(out, fmt.Sprintf("%s@%d=%s", userKey, seq, value))
	}
}

func TestMergeCursorInterleavesSourcesInKeyOrder(t *testing.T) {
	merged := newMergeCursor([]core.Cursor{
		&sliceCursor{entries: []entry{set("a", 3, "a3"), set("c", 1, "c1")}},
		&sliceCursor{entries: []entry{set("a", 2, "a2"), set("b", 5, "b5")}},
		&sliceCursor{},
	})

	assert.Equal(t, []string{"a@3=a3", "a@2=a2", "b@5=b5", "c@1=c1"}, drain(t, merged))
}

func TestMergeCursorMatchesStableSortForAnySourceCount(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for k := 1; k <= 17; k++ {
		for round := range 50 {
			var want []string
			sources := make([]core.Cursor, k)
			for i := range sources {
				var entries []entry
				for range rng.IntN(20) { // some sources stay empty
					entries = append(entries, set(fmt.Sprintf("k%02d", rng.IntN(30)), 1, fmt.Sprint(i)))
				}
				slices.SortStableFunc(entries, func(a, b entry) int { return bytes.Compare(a.key, b.key) })
				for _, e := range entries {
					want = append(want, fmt.Sprintf("%s@1=%s", e.key.UserKey(), e.value))
				}
				sources[i] = &sliceCursor{entries: entries}
			}
			// stable sort keeps source order for equal keys
			slices.SortStableFunc(want, func(a, b string) int { return strings.Compare(a[:3], b[:3]) })

			got := drain(t, newMergeCursor(sources))
			require.Equal(t, want, got, "k=%d round=%d", k, round)
		}
	}
}

func TestMVCCCursorResolvesNewestVisibleVersionPerKey(t *testing.T) {
	sources := func() []core.Cursor {
		return []core.Cursor{
			&sliceCursor{entries: []entry{set("a", 9, "a9"), del("b", 6), set("c", 4, "c4")}},
			&sliceCursor{entries: []entry{set("a", 5, "a5"), set("b", 3, "b3"), set("c", 2, "c2")}},
		}
	}

	// a@9 is above readSeq and b's tombstone hides b@3
	reader := newMVCCCursor(newMergeCursor(sources()), 7, false)
	assert.Equal(t, []string{"a@5=a5", "c@4=c4"}, drain(t, reader))

	// compaction keeps the tombstone so b@3 can't resurface below
	compaction := newMVCCCursor(newMergeCursor(sources()), math.MaxUint64, true)
	assert.Equal(t, []string{"a@9=a9", "b@6=deleted", "c@4=c4"}, drain(t, compaction))
}

func TestMergeCursorStopsOnSourceError(t *testing.T) {
	broken := errors.New("broken block")
	cur := newMVCCCursor(newMergeCursor([]core.Cursor{
		&sliceCursor{entries: []entry{set("a", 1, "a1"), set("z", 1, "z1")}},
		&sliceCursor{entries: []entry{set("b", 1, "b1")}, err: broken},
	}), math.MaxUint64, false)

	var keys []string
	for {
		key, _, ok := cur.Next()
		if !ok {
			break
		}
		keys = append(keys, string(quxKey(key).UserKey()))
	}

	assert.Equal(t, []string{"a", "b"}, keys)
	assert.ErrorIs(t, cur.Err(), broken)
}
