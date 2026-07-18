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
	assert.Nil(t, m.arena)
	assert.Zero(t, m.head)
	assert.Zero(t, m.height)
	assert.Equal(t, [slMaxHeight]uint32{}, m.tail)
}
