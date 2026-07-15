package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()

			// 1. Set / Get
			key := []byte("foo")
			val := []byte("bar")
			m.Set(key, val)

			got, ok := m.Get(key)
			assert.True(t, ok, "Get failed: key not found")
			assert.Equal(t, val, got, "Get returned wrong value")

			// 2. Overwrite
			newVal := []byte("baz")
			m.Set(key, newVal)

			got, ok = m.Get(key)
			assert.True(t, ok, "Get failed after overwrite: key not found")
			assert.Equal(t, newVal, got, "Get returned wrong value after overwrite")

			// 4. Delete
			// m.Delete(key)

			// _, ok = m.Get(key)
			// assert.False(t, ok, "Get returned ok=true after delete")

			// 5. Len
			m2 := impl.Factory()
			assert.Equal(t, 0, m2.Len(), "Len not 0 for new table")

			for i := range 100 {
				m2.Set(fmt.Appendf(nil, "key-%03d", i), []byte("val"))
			}
			assert.Equal(t, 100, m2.Len(), "Len expected 100")

			// 6. Cursor
			t.Run("Cursor", func(t *testing.T) {
				allCount := 0
				c := m2.Cursor(nil, nil)
				for {
					_, _, ok := c.Next()
					if !ok {
						break
					}
					allCount++
				}
				assert.Equal(t, 100, allCount, "All cursor expected 100 items")
			})
		})
	}
}

func TestMemtableCursor(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()

			// 1. Manual keys in random order
			keys := []string{"banana", "apple", "cherry", "date", "apricot", "berry", "applepie", "a", "b", "c"}
			for _, k := range keys {
				m.Set([]byte(k), []byte("val"))
			}

			var iterated []string
			c := m.Cursor(nil, nil)
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				iterated = append(iterated, string(k))
			}

			expected := slices.Clone(keys)
			slices.Sort(expected)
			assert.Equal(t, expected, iterated, "Keys are not sorted")

			seekKey, seekVal, ok := m.Seek([]byte("applep"))
			assert.True(t, ok, "Seek should find first key >= seek key")
			assert.Equal(t, []byte("applepie"), seekKey)
			assert.Equal(t, []byte("val"), seekVal)

			var fromApple []string
			c = m.Cursor([]byte("apple"), nil)
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				fromApple = append(fromApple, string(k))
			}
			assert.Equal(t, []string{"apple", "applepie", "apricot", "b", "banana", "berry", "c", "cherry", "date"}, fromApple, "Cursor should include first key >= start")

			var fromBetweenKeys []string
			c = m.Cursor([]byte("bb"), nil)
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				fromBetweenKeys = append(fromBetweenKeys, string(k))
			}
			assert.Equal(t, []string{"berry", "c", "cherry", "date"}, fromBetweenKeys, "Cursor should start at next key when start is absent")

			var ranged []string
			c = m.Cursor([]byte("apple"), []byte("c"))
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				ranged = append(ranged, string(k))
			}
			assert.Equal(t, []string{"apple", "applepie", "apricot", "b", "banana", "berry", "c"}, ranged, "Cursor should include both bounds")

			var emptyRange []string
			c = m.Cursor([]byte("d"), []byte("a"))
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				emptyRange = append(emptyRange, string(k))
			}
			assert.Empty(t, emptyRange, "Cursor should be empty when start is after end")

			// 2. Larger randomized test
			m2 := impl.Factory()
			r := rand.New(rand.NewSource(42))
			count := 2000
			uniqueKeys := make(map[string]struct{})
			for range count {
				keyLen := r.Intn(32) + 1
				key := make([]byte, keyLen)
				r.Read(key)
				m2.Set(key, []byte("val"))
				uniqueKeys[string(key)] = struct{}{}
			}

			var lastKey []byte
			iterCount := 0
			c = m2.Cursor(nil, nil)
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				if lastKey != nil {
					assert.Greater(t, bytes.Compare(k, lastKey), 0, "Cursor out of order at position %d: %x -> %x", iterCount, lastKey, k)
				}
				lastKey = bytes.Clone(k)
				iterCount++
			}

			assert.Equal(t, len(uniqueKeys), iterCount, "Cursor returned wrong number of unique keys")
		})
	}
}

func TestMemtableBorrowedViewsAreCapacityCapped(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()
			require.NoError(t, m.Set([]byte("a"), []byte("value-a")))
			require.NoError(t, m.Set([]byte("b"), []byte("value-b")))

			value, ok := m.Get([]byte("a"))
			require.True(t, ok)
			assert.Equal(t, len(value), cap(value))

			key, value, ok := m.Seek([]byte("a"))
			require.True(t, ok)
			assert.Equal(t, len(key), cap(key))
			assert.Equal(t, len(value), cap(value))

			start := make([]byte, 1, 16)
			end := make([]byte, 1, 16)
			start[0], end[0] = 'a', 'b'
			cursor := m.Cursor(start, end)
			key, value, ok = cursor.Next()
			require.True(t, ok)
			assert.Equal(t, len(key), cap(key))
			assert.Equal(t, len(value), cap(value))

			switch cursor := cursor.(type) {
			case *slCursor:
				assert.Equal(t, len(cursor.start), cap(cursor.start))
				assert.Equal(t, len(cursor.end), cap(cursor.end))
			case *btreeCursor:
				assert.Equal(t, len(cursor.start), cap(cursor.start))
				assert.Equal(t, len(cursor.end), cap(cursor.end))
			default:
				t.Fatalf("unexpected cursor type %T", cursor)
			}
		})
	}
}

func TestMemtableSizeBytesReportsLiveData(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()
			assert.Zero(t, m.SizeBytes())

			require.NoError(t, m.Set([]byte("abc"), []byte("12")))
			assert.Equal(t, 5, m.SizeBytes())

			require.NoError(t, m.Set([]byte("abc"), []byte("12345")))
			assert.Equal(t, 8, m.SizeBytes())

			require.NoError(t, m.Set([]byte("abc"), []byte("x")))
			assert.Equal(t, 4, m.SizeBytes())

			require.NoError(t, m.Set([]byte("z"), []byte("vv")))
			assert.Equal(t, 7, m.SizeBytes())
			assert.Equal(t, 2, m.Len())
		})
	}
}

func TestMemtableClearReleasesStorageAndAllowsReuse(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()
			for i := range 200 {
				require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("value")))
			}
			require.NotZero(t, m.Len())
			require.NotZero(t, m.SizeBytes())

			m.Clear()
			assert.Zero(t, m.Len())
			assert.Zero(t, m.SizeBytes())
			_, ok := m.Get([]byte("key-000"))
			assert.False(t, ok)
			_, _, ok = m.Seek(nil)
			assert.False(t, ok)
			_, _, ok = m.Cursor(nil, nil).Next()
			assert.False(t, ok)

			switch m := m.(type) {
			case *slMemtable:
				assert.Nil(t, m.arena)
				assert.Zero(t, m.head)
				assert.Zero(t, m.height)
				assert.Equal(t, [slMaxHeight]uint32{}, m.tail)
			case *btreeMemtable:
				assert.Nil(t, m.data)
				assert.Nil(t, m.leafChunks)
				assert.Nil(t, m.internalChunks)
				assert.Zero(t, m.root)
				assert.Zero(t, m.firstLeaf)
				assert.Zero(t, m.lastLeaf)
				assert.Zero(t, m.leafCount)
				assert.Zero(t, m.internalCount)
			default:
				t.Fatalf("unexpected memtable type %T", m)
			}

			m.Clear() // idempotent
			require.NoError(t, m.Set([]byte("new"), []byte("value")))
			value, ok := m.Get([]byte("new"))
			require.True(t, ok)
			assert.Equal(t, []byte("value"), value)
			assert.Equal(t, 1, m.Len())
			assert.Equal(t, len("new")+len("value"), m.SizeBytes())
			_, ok = m.Get([]byte("key-000"))
			assert.False(t, ok)
		})
	}
}

func TestMemtableClearPreservesComparator(t *testing.T) {
	reverse := func(a, b []byte) int { return bytes.Compare(b, a) }
	for _, typ := range []MemTableType{Skiplist, BTree} {
		t.Run(typ.String(), func(t *testing.T) {
			m := New(typ, WithComparator(reverse))
			require.NoError(t, m.Set([]byte("a"), nil))
			m.Clear()
			require.NoError(t, m.Set([]byte("a"), nil))
			require.NoError(t, m.Set([]byte("b"), nil))

			key, _, ok := m.Cursor(nil, nil).Next()
			require.True(t, ok)
			assert.Equal(t, []byte("b"), key)
		})
	}
}

func TestBTreeUpdateDoesNotSplitFullNodes(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		m := newBTreeMemtable().(*btreeMemtable)
		for i := range btreeLeafMaxItems {
			require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
		}

		root := m.root
		leafCount := m.leafCount
		internalCount := m.internalCount
		require.NoError(t, m.Set([]byte("key-010"), []byte("updated")))

		assert.Equal(t, root, m.root)
		assert.Equal(t, leafCount, m.leafCount)
		assert.Equal(t, internalCount, m.internalCount)
		assert.Empty(t, m.internalChunks)
		value, ok := m.Get([]byte("key-010"))
		require.True(t, ok)
		assert.Equal(t, []byte("updated"), value)
	})

	t.Run("Child", func(t *testing.T) {
		m := newBTreeMemtable().(*btreeMemtable)
		for i := range btreeLeafMaxItems + 1 {
			require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
		}

		leafCount := m.leafCount
		internalCount := m.internalCount
		require.NoError(t, m.Set([]byte("key-010"), []byte("updated")))

		assert.Equal(t, leafCount, m.leafCount)
		assert.Equal(t, internalCount, m.internalCount)
		value, ok := m.Get([]byte("key-010"))
		require.True(t, ok)
		assert.Equal(t, []byte("updated"), value)
	})
}

func TestBTreeRootSplitCapacityFailureIsAtomic(t *testing.T) {
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range btreeLeafMaxItems {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
	}

	m.internalCount = btreeLeafBit
	root := m.root
	leafCount := m.leafCount
	dataLen := m.dataLen
	sizeBytes := m.sizeBytes
	count := m.count

	err := m.Set([]byte("key-999"), []byte("v"))
	require.ErrorIs(t, err, ErrMemtableFull)
	assert.Equal(t, root, m.root)
	assert.Equal(t, leafCount, m.leafCount)
	assert.Equal(t, uint32(btreeLeafBit), m.internalCount)
	assert.Equal(t, dataLen, m.dataLen)
	assert.Equal(t, sizeBytes, m.sizeBytes)
	assert.Equal(t, count, m.count)
	assert.Equal(t, uint16(btreeLeafMaxItems), m.leaf(root.index()).n)
}

func TestBTreeChunksAreSmallLazyAndDenseForAscendingInserts(t *testing.T) {
	m := newBTreeMemtable().(*btreeMemtable)
	assert.Equal(t, 128, btreeLeafChunkSize)
	assert.Equal(t, 32, btreeInternalChunkSize)
	assert.Len(t, m.leafChunks, 1)
	assert.Empty(t, m.internalChunks)

	for i := range btreeLeafMaxItems + 1 {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
	}

	leftIdx := m.firstLeaf
	left := m.leaf(leftIdx)
	rightIdx := left.nextLeaf
	right := m.leaf(rightIdx)
	assert.Equal(t, uint16(btreeLeafMaxItems), left.n)
	assert.Equal(t, uint16(1), right.n)
	assert.Equal(t, rightIdx, m.lastLeaf)
	assert.Equal(t, uint32(3), m.leafCount) // nil + two leaves
	assert.Len(t, m.internalChunks, 1)
}

func TestBTreeAscendingInsertsAcrossInternalSplits(t *testing.T) {
	const count = 10_000
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range count {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%05d", i), []byte("v")))
	}

	expectedLeaves := (count + btreeLeafMaxItems - 1) / btreeLeafMaxItems
	assert.Equal(t, uint32(expectedLeaves+1), m.leafCount) // include nil leaf

	leafCount := 0
	for leafIdx := m.firstLeaf; leafIdx != 0; leafIdx = m.leaf(leafIdx).nextLeaf {
		leafCount++
		if leafIdx != m.lastLeaf {
			assert.Equal(t, uint16(btreeLeafMaxItems), m.leaf(leafIdx).n)
		}
	}
	assert.Equal(t, expectedLeaves, leafCount)

	cursor := m.Cursor(nil, nil)
	for i := range count {
		key, value, ok := cursor.Next()
		require.True(t, ok, "cursor ended at item %d", i)
		assert.Equal(t, fmt.Sprintf("key-%05d", i), string(key))
		assert.Equal(t, []byte("v"), value)
	}
	_, _, ok := cursor.Next()
	assert.False(t, ok)
}

func TestBTreeCanRevisitFullLeafFromBiasedSplit(t *testing.T) {
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range btreeLeafMaxItems + 1 {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
	}
	require.NoError(t, m.Set([]byte("key-010a"), []byte("middle")))

	value, ok := m.Get([]byte("key-010a"))
	require.True(t, ok)
	assert.Equal(t, []byte("middle"), value)
	assert.Equal(t, btreeLeafMaxItems+2, m.Len())

	var previous []byte
	seen := 0
	cursor := m.Cursor(nil, nil)
	for {
		key, _, ok := cursor.Next()
		if !ok {
			break
		}
		if previous != nil {
			assert.Less(t, bytes.Compare(previous, key), 0)
		}
		previous = bytes.Clone(key)
		seen++
	}
	assert.Equal(t, m.Len(), seen)
}

func TestBTreeCursorSkipsEmptyNonFinalLeaf(t *testing.T) {
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range btreeLeafMaxItems + 1 {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
	}

	m.leaf(m.firstLeaf).n = 0
	cursor := m.Cursor(nil, nil)
	key, value, ok := cursor.Next()
	require.True(t, ok)
	assert.Equal(t, []byte("key-063"), key)
	assert.Equal(t, []byte("v"), value)
	_, _, ok = cursor.Next()
	assert.False(t, ok)
}

func TestMemtableRejectsNilComparator(t *testing.T) {
	for _, typ := range []MemTableType{Skiplist, BTree} {
		t.Run(typ.String(), func(t *testing.T) {
			assert.PanicsWithValue(t, "memtable: nil comparator", func() {
				New(typ, WithComparator(nil))
			})
		})
	}
}

func TestMemtableComparator(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory(WithComparator(func(a, b []byte) int {
				if c := bytes.Compare(a[:1], b[:1]); c != 0 {
					return c
				}
				return int(b[1]) - int(a[1])
			}))

			m.Set([]byte{'a', 3}, []byte("a3"))
			m.Set([]byte{'a', 1}, []byte("a1"))
			m.Set([]byte{'b', 2}, []byte("b2"))
			m.Set([]byte{'b', 1}, []byte("b1"))

			var iterated [][]byte
			c := m.Cursor(nil, nil)
			for {
				k, _, ok := c.Next()
				if !ok {
					break
				}
				iterated = append(iterated, bytes.Clone(k))
			}
			assert.Equal(t, [][]byte{
				{'a', 3},
				{'a', 1},
				{'b', 2},
				{'b', 1},
			}, iterated)

			key, val, ok := m.Seek([]byte{'a', 2})
			assert.True(t, ok)
			assert.Equal(t, []byte{'a', 1}, key)
			assert.Equal(t, []byte("a1"), val)

			key, val, ok = m.Seek([]byte{'b', 3})
			assert.True(t, ok)
			assert.Equal(t, []byte{'b', 2}, key)
			assert.Equal(t, []byte("b2"), val)

			key, _, ok = m.Seek([]byte{'a', 0})
			assert.True(t, ok)
			assert.Equal(t, []byte{'b', 2}, key)
		})
	}
}

func TestMemtableConcurrency(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()
			const numGoroutines = 50
			const insertsPerGoroutine = 1000

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for g := range numGoroutines {
				go func(gid int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))
					for i := range insertsPerGoroutine {
						// Use keys that naturally hit different shards and some same shards
						// Key format: "key-<shard-byte>-<random>"
						key := fmt.Appendf(nil, "key-%d-%d", rng.Intn(256), i)
						m.Set(key, []byte("value"))

						// Occasionally read back
						if i%10 == 0 {
							_, _ = m.Get(key)
						}
					}
				}(g)
			}

			wg.Wait()

			total := m.Len()
			assert.Greater(t, total, 0, "Len is 0 after concurrent inserts")
		})
	}
}

func TestBTreeCursorReleasesReadLockBetweenNextCalls(t *testing.T) {
	m := newBTreeMemtable()
	expected := make([]string, 0, btreeLeafMaxItems)

	for i := range btreeLeafMaxItems {
		key := fmt.Sprintf("key-%03d", i)
		expected = append(expected, key)
		assert.NoError(t, m.Set([]byte(key), []byte("value")))
	}

	reachedNext := make(chan struct{})
	releaseNext := make(chan struct{})
	cursorDone := make(chan []string, 1)

	go func() {
		got := make([]string, 0, btreeLeafMaxItems)
		c := m.Cursor(nil, nil)

		key, _, ok := c.Next()
		if ok {
			got = append(got, string(key))
		}
		close(reachedNext)
		<-releaseNext

		for {
			key, _, ok = c.Next()
			if !ok {
				break
			}
			got = append(got, string(key))
		}

		cursorDone <- got
	}()

	<-reachedNext

	writeDone := make(chan error, 1)
	go func() {
		// The root leaf is full, so this write also forces a split while the
		// cursor is paused between calls to Next.
		writeDone <- m.Set([]byte("key-999"), []byte("value"))
	}()

	select {
	case err := <-writeDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		close(releaseNext)
		t.Fatal("write blocked while cursor was paused between Next calls")
	}

	close(releaseNext)
	assert.Equal(t, expected, <-cursorDone)
}

const (
	benchUserKeyLen                  = 16
	benchInternalKeyTrailerLen       = 8 + 1
	benchOpSet                 uint8 = 1
	benchOpSeekMax             uint8 = 0xff
	benchCursorKeys                  = 10_000
)

func makeInternalBenchKey(userKey []byte, seq uint64, op uint8) []byte {
	key := make([]byte, len(userKey)+benchInternalKeyTrailerLen)
	copy(key, userKey)
	binary.LittleEndian.PutUint64(key[len(userKey):len(userKey)+8], seq)
	key[len(userKey)+8] = op
	return key
}

func compareInternalBenchKey(a, b []byte) int {
	aUserLen := len(a) - benchInternalKeyTrailerLen
	bUserLen := len(b) - benchInternalKeyTrailerLen

	if c := bytes.Compare(a[:aUserLen], b[:bUserLen]); c != 0 {
		return c
	}

	aSeq := binary.LittleEndian.Uint64(a[aUserLen : aUserLen+8])
	bSeq := binary.LittleEndian.Uint64(b[bUserLen : bUserLen+8])
	if aSeq > bSeq {
		return -1
	}
	if aSeq < bSeq {
		return 1
	}

	aOp := a[aUserLen+8]
	bOp := b[bUserLen+8]
	if aOp > bOp {
		return -1
	}
	if aOp < bOp {
		return 1
	}
	return 0
}

func BenchmarkMemtable(b *testing.B) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	val := []byte("value")

	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			b.Run("Set/Unique/Seq", func(b *testing.B) {
				m := impl.Factory()
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}
			})

			b.Run("Set/Unique/Rand", func(b *testing.B) {
				m := impl.Factory()
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}
			})

			b.Run("Get/Seq", func(b *testing.B) {
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Get(keys[i])
				}
			})

			b.Run("Get/Rand", func(b *testing.B) {
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				indices := make([]int, b.N)
				for i := 0; i < b.N; i++ {
					indices[i] = rngLocal.Intn(b.N)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Get(keys[indices[i]])
				}
			})

			b.Run("Seek/Seq", func(b *testing.B) {
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Seek(keys[i])
				}
			})

			b.Run("Seek/Rand", func(b *testing.B) {
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				indices := make([]int, b.N)
				for i := 0; i < b.N; i++ {
					indices[i] = rngLocal.Intn(b.N)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Seek(keys[indices[i]])
				}
			})

			b.Run("Cursor/All", func(b *testing.B) {
				m := impl.Factory()
				for i := range benchCursorKeys {
					m.Set(fmt.Appendf(nil, "unique-seq:%016d", i), val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					c := m.Cursor(nil, nil)
					for {
						_, _, ok := c.Next()
						if !ok {
							break
						}
						count++
					}
					if count != benchCursorKeys {
						b.Fatalf("expected %d, got %d", benchCursorKeys, count)
					}
				}
			})

			b.Run("CustomKey", func(b *testing.B) {
				b.Run("Set/Unique/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					userKey := []byte("bench-user-key-0")
					keys := make([][]byte, b.N)
					for i := range b.N {
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}
				})

				b.Run("Set/Unique/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}
				})

				b.Run("Get/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					keys := make([][]byte, b.N)
					for i := range b.N {
						keys[i] = makeInternalBenchKey(fmt.Appendf(nil, "unique-seq:%016d", i), uint64(i)+1, benchOpSet)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Get(keys[i])
					}
				})

				b.Run("Get/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					indices := make([]int, b.N)
					for i := 0; i < b.N; i++ {
						indices[i] = rngLocal.Intn(b.N)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Get(keys[indices[i]])
					}
				})

				b.Run("Seek/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					keys := make([][]byte, b.N)
					seekKeys := make([][]byte, b.N)
					for i := range b.N {
						userKey := fmt.Appendf(nil, "unique-seq:%016d", i)
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
						seekKeys[i] = makeInternalBenchKey(userKey, uint64(b.N)+1, benchOpSeekMax)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Seek(seekKeys[i])
					}
				})

				b.Run("Seek/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					seekKeys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
						seekKeys[i] = makeInternalBenchKey(userKey, uint64(b.N)+1, benchOpSeekMax)
					}
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					indices := make([]int, b.N)
					for i := 0; i < b.N; i++ {
						indices[i] = rngLocal.Intn(b.N)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Seek(seekKeys[indices[i]])
					}
				})

				b.Run("Cursor/All", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					for i := range benchCursorKeys {
						userKey := fmt.Appendf(nil, "unique-seq:%016d", i)
						m.Set(makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet), val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						count := 0
						c := m.Cursor(nil, nil)
						for {
							_, _, ok := c.Next()
							if !ok {
								break
							}
							count++
						}
						if count != benchCursorKeys {
							b.Fatalf("expected %d, got %d", benchCursorKeys, count)
						}
					}
				})
			})
		})
	}
}
