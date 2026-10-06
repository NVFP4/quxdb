package memtable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBTreeUpdate(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		m := newBTreeMemtable().(*btreeMemtable)
		for i := range btreeLeafMaxItems {
			require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("v")))
		}

		root := m.root.Load()
		leafCount := m.leafCount
		internalCount := m.internalCount
		require.NoError(t, m.Set([]byte("key-010"), []byte("updated")))

		assert.Equal(t, root, m.root.Load())
		assert.Equal(t, leafCount, m.leafCount)
		assert.Equal(t, internalCount, m.internalCount)
		assert.Nil(t, m.internalChunks[0])
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

func TestBTreeUpdateAppendOnly(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	updated := []byte("new")
	m := newBTreeMemtable(
		WithCapacityBytes(btreeRecordLen(len(key), len(value)) + btreeRecordLen(len(key), len(updated))),
	).(*btreeMemtable)

	require.NoError(t, m.Set(key, value))
	dataLen := m.dataLen
	// updates append a full record, the old one stays readable
	require.NoError(t, m.Set(key, updated))
	assert.Equal(t, dataLen+btreeRecordLen(len(key), len(updated)), m.dataLen)
	assert.Equal(t, len(key)+len(updated), m.SizeBytes())

	require.ErrorIs(t, m.Set(key, []byte("x")), ErrMemtableFull)
	got, ok := m.Get(key)
	require.True(t, ok)
	assert.Equal(t, updated, got)
}

func TestBTreeCapacityCountsNodeChunks(t *testing.T) {
	const capacity = 1 << 20
	for _, order := range []string{"ascending", "random"} {
		t.Run(order, func(t *testing.T) {
			m := newBTreeMemtable(WithCapacityBytes(capacity)).(*btreeMemtable)
			ids := rand.New(rand.NewPCG(1, 2)).Perm(1 << 20)
			for i := 0; ; i++ {
				id := i
				if order == "random" {
					id = ids[i]
				}
				// small pairs make the tree a large share of the memory
				if err := m.Set(fmt.Appendf(nil, "k%07d", id), []byte("v")); err != nil {
					require.ErrorIs(t, err, ErrMemtableFull)
					break
				}
			}

			allocated := -btreeLeafChunkBytes // the first leaf chunk is free
			for _, c := range m.leafChunks {
				if c != nil {
					allocated += btreeLeafChunkBytes
				}
			}
			for _, c := range m.internalChunks {
				if c != nil {
					allocated += btreeInternalChunkBytes
				}
			}
			require.Positive(t, allocated)
			assert.Equal(t, allocated, m.indexBytes)
			assert.LessOrEqual(t, m.dataLen+m.indexBytes, capacity)
		})
	}
}

func TestBTreeAscending(t *testing.T) {
	const count = 10_000
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range count {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%05d", i), []byte("v")))
	}

	expectedLeaves := (count + btreeLeafMaxItems - 1) / btreeLeafMaxItems
	assert.Equal(t, uint32(expectedLeaves+1), m.leafCount)

	leafCount := 0
	for leafIdx := m.firstLeaf; leafIdx != 0; leafIdx = m.leaf(leafIdx).nextLeaf.Load() {
		leafCount++
		if leafIdx != m.lastLeaf {
			assert.Equal(t, uint32(btreeLeafMaxItems), m.leaf(leafIdx).n.Load())
		}
	}
	assert.Equal(t, expectedLeaves, leafCount)

	it := m.Iterator(nil, nil)
	for i := range count {
		key, value, ok := it.Next()
		require.True(t, ok, "iterator ended at item %d", i)
		assert.Equal(t, fmt.Sprintf("key-%05d", i), string(key))
		assert.Equal(t, []byte("v"), value)
	}
	_, _, ok := it.Next()
	assert.False(t, ok)
}

func TestBTreeMiddleInsert(t *testing.T) {
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
	it := m.Iterator(nil, nil)
	for {
		key, _, ok := it.Next()
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

func TestBTreeIteratorWriterProgress(t *testing.T) {
	m := newBTreeMemtable()
	expected := make([]string, 0, btreeLeafMaxItems)
	for i := range btreeLeafMaxItems {
		key := fmt.Sprintf("key-%03d", i)
		expected = append(expected, key)
		require.NoError(t, m.Set([]byte(key), []byte("value")))
	}

	reachedNext := make(chan struct{})
	releaseNext := make(chan struct{})
	iterDone := make(chan []string, 1)
	go func() {
		got := make([]string, 0, btreeLeafMaxItems)
		it := m.Iterator(nil, nil)
		key, _, ok := it.Next()
		if ok {
			got = append(got, string(key))
		}
		close(reachedNext)
		<-releaseNext
		for {
			key, _, ok = it.Next()
			if !ok {
				break
			}
			got = append(got, string(key))
		}
		iterDone <- got
	}()

	<-reachedNext
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- m.Set([]byte("key-999"), []byte("value"))
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(releaseNext)
		t.Fatal("write blocked while iterator was paused")
	}

	close(releaseNext)
	assert.Equal(t, expected, <-iterDone)
}

func TestBTreeClear(t *testing.T) {
	m := newBTreeMemtable().(*btreeMemtable)
	for i := range 200 {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("value")))
	}

	m.Clear()
	assert.Nil(t, m.data)
	assert.Nil(t, m.leafChunks)
	assert.Nil(t, m.internalChunks)
	assert.Zero(t, m.root.Load())
	assert.Zero(t, m.firstLeaf)
	assert.Zero(t, m.lastLeaf)
	assert.Zero(t, m.leafCount)
	assert.Zero(t, m.internalCount)
}
