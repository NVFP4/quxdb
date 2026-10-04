package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoderLatchesFirstError(t *testing.T) {
	d := NewDecoder([]byte{0x05, 0x01, 0x02, 0x80})

	assert.Equal(t, uint64(5), d.UVarint("a"))
	assert.Nil(t, d.Bytes("b", 4))
	first := d.Err()
	require.Error(t, first)

	// input remains but every read after the error fails
	assert.Zero(t, d.UVarint("c"))
	assert.Nil(t, d.Bytes("d", 1))
	assert.Zero(t, d.Uint16("e"))
	assert.Zero(t, d.Uint32At("f", 0))
	assert.Equal(t, first, d.Err())
	assert.Equal(t, 1, d.Offset())
}

func TestDecoderRejectsBadLengthsAndVarints(t *testing.T) {
	for name, read := range map[string]func(*Decoder){
		"negative length":   func(d *Decoder) { d.Bytes("x", -1) },
		"truncated uvarint": func(d *Decoder) { d.UVarint("x") },
		"uint32 past end":   func(d *Decoder) { d.Uint32At("x", 2) },
	} {
		t.Run(name, func(t *testing.T) {
			d := NewDecoder([]byte{0x80, 0x80, 0x80})
			read(d)
			assert.Error(t, d.Err())
		})
	}
}

func TestDecoderMultiByteUvarint(t *testing.T) {
	d := NewDecoder([]byte{0xac, 0x02, 0x7f})
	assert.Equal(t, uint64(300), d.UVarint("a"))
	assert.Equal(t, uint64(127), d.UVarint("b"))
	require.NoError(t, d.Err())
	assert.Equal(t, 3, d.Offset())
}
