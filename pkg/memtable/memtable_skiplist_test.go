package memtable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkiplistClear(t *testing.T) {
	m := newSkiplistMemtable()
	for i := range 200 {
		require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("value")))
	}

	m.Clear()
	assert.Nil(t, m.arena.Load())
	assert.Equal(t, [slMaxHeight]uint32{}, m.tail)
}

func TestSkiplistCapacityCountsNodes(t *testing.T) {
	const capacity = 1 << 20
	m := newSkiplistMemtable(WithCapacityBytes(capacity))
	for i := 0; ; i++ {
		// small pairs make nodes a large share of the memory
		if err := m.Set(fmt.Appendf(nil, "k%07d", i), []byte("v")); err != nil {
			require.ErrorIs(t, err, ErrMemtableFull)
			break
		}
	}

	assert.Equal(t, m.len*slNodeBytes, m.indexBytes)
	a := m.arena.Load()
	assert.LessOrEqual(t, a.dataLen+m.indexBytes, capacity)
	assert.Greater(t, a.dataLen+m.indexBytes, capacity-slPairBytes(8, 1))
}
