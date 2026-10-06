package codec

import (
	"bytes"
	"encoding/binary"
	"math"
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
	short := []byte{0x80, 0x80, 0x80}
	for name, tc := range map[string]struct {
		in   []byte
		read func(*Decoder)
	}{
		"negative length":      {short, func(d *Decoder) { d.Bytes("x", -1) }},
		"length overflows off": {short, func(d *Decoder) { d.Uint16("w"); d.Bytes("x", math.MaxInt) }},
		"length past end":      {short, func(d *Decoder) { d.Bytes("x", 4) }},
		"truncated uvarint":    {short, func(d *Decoder) { d.UVarint("x") }},
		"uvarint overflow":     {bytes.Repeat([]byte{0xff}, 11), func(d *Decoder) { d.UVarint("x") }},
		"uvarint at end":       {nil, func(d *Decoder) { d.UVarint("x") }},
		"uint64 past end":      {short, func(d *Decoder) { d.Uint64("x") }},
		"uint32 past end":      {short, func(d *Decoder) { d.Uint32At("x", 2) }},
		"uint32 before start":  {short, func(d *Decoder) { d.Uint32At("x", -4) }},
		"uint32 at max offset": {short, func(d *Decoder) { d.Uint32At("x", math.MaxInt) }},
	} {
		t.Run(name, func(t *testing.T) {
			d := NewDecoder(tc.in)
			tc.read(d)
			require.Error(t, d.Err())
			assert.Contains(t, d.Err().Error(), `"x"`)
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

func TestDecoderUvarintMaxValue(t *testing.T) {
	d := NewDecoder(binary.AppendUvarint(nil, math.MaxUint64))
	assert.Equal(t, uint64(math.MaxUint64), d.UVarint("x"))
	require.NoError(t, d.Err())
	assert.Zero(t, d.Remaining())
}

func TestDecoderFixedWidthByteOrder(t *testing.T) {
	src := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	for name, tc := range map[string]struct {
		read func(*Decoder) uint64
		want uint64
		size int
	}{
		"uint16":   {func(d *Decoder) uint64 { return uint64(d.Uint16("x")) }, 0x0201, 2},
		"uint16be": {func(d *Decoder) uint64 { return uint64(d.Uint16BE("x")) }, 0x0102, 2},
		"uint32":   {func(d *Decoder) uint64 { return uint64(d.Uint32("x")) }, 0x04030201, 4},
		"uint32be": {func(d *Decoder) uint64 { return uint64(d.Uint32BE("x")) }, 0x01020304, 4},
		"uint64":   {func(d *Decoder) uint64 { return d.Uint64("x") }, 0x0807060504030201, 8},
	} {
		t.Run(name, func(t *testing.T) {
			d := NewDecoder(src)
			assert.Equal(t, tc.want, tc.read(d))
			require.NoError(t, d.Err())
			assert.Equal(t, tc.size, d.Offset())
		})
	}
}

func TestDecoderUint32AtIsPositional(t *testing.T) {
	d := NewDecoder([]byte{0xaa, 0x01, 0x02, 0x03, 0x04})

	// the last four bytes are in range and the cursor does not move
	assert.Equal(t, uint32(0x04030201), d.Uint32At("x", 1))
	require.NoError(t, d.Err())
	assert.Zero(t, d.Offset())
	assert.Equal(t, []byte{0xaa}, d.Bytes("y", 1))
}

func TestDecoderBytesAliasInput(t *testing.T) {
	src := []byte{0x01, 0x02, 0x03}
	d := NewDecoder(src)

	b := d.Bytes("x", 2)
	src[0] = 0xff
	assert.Equal(t, []byte{0xff, 0x02}, b)

	// a zero length read at the end is valid, not a failure
	d.Bytes("y", 1)
	assert.NotNil(t, d.Bytes("z", 0))
	require.NoError(t, d.Err())
	assert.Zero(t, d.Remaining())
}

func FuzzDecoder(f *testing.F) {
	f.Add([]byte{}, []byte{0, 1, 2})
	f.Add([]byte{0xac, 0x02, 0x7f}, []byte{0, 0, 0})
	f.Add(bytes.Repeat([]byte{0xff}, 11), []byte{0, 6, 7})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{1, 0, 2, 3, 4, 5, 6, 0x17, 0x27})
	f.Add(binary.AppendUvarint(nil, math.MaxUint64), []byte{0, 1, 0xff})

	// operands chosen to hit boundaries and integer overflow
	operand := func(sel byte, remaining, n int) int {
		return [...]int{0, 1, -1, remaining, remaining + 1, remaining - 4, n, n - 4, math.MaxInt, math.MinInt, math.MaxInt - 3}[int(sel)%11]
	}

	f.Fuzz(func(t *testing.T, data, ops []byte) {
		d := NewDecoder(data)
		for _, op := range ops {
			prevOff, prevErr := d.Offset(), d.Err()
			sel := op >> 3

			switch op & 7 {
			case 0:
				v := d.UVarint("uvarint")
				if d.Err() == nil {
					want, n := binary.Uvarint(data[prevOff:])
					if n <= 0 || v != want || d.Offset() != prevOff+n {
						t.Fatalf("uvarint at %d = %d, offset %d", prevOff, v, d.Offset())
					}
				}
			case 1:
				n := operand(sel, len(data)-prevOff, len(data))
				b := d.Bytes("bytes", n)
				if d.Err() == nil && (!bytes.Equal(b, data[prevOff:prevOff+n]) || d.Offset() != prevOff+n) {
					t.Fatalf("bytes(%d) at %d = %x", n, prevOff, b)
				}
				if d.Err() != nil && b != nil {
					t.Fatalf("bytes(%d) returned data with error %v", n, d.Err())
				}
			case 2:
				v := d.Uint16("u16")
				if d.Err() == nil && v != binary.LittleEndian.Uint16(data[prevOff:]) {
					t.Fatalf("uint16 at %d = %#x", prevOff, v)
				}
			case 3:
				v := d.Uint16BE("u16be")
				if d.Err() == nil && v != binary.BigEndian.Uint16(data[prevOff:]) {
					t.Fatalf("uint16be at %d = %#x", prevOff, v)
				}
			case 4:
				v := d.Uint32("u32")
				if d.Err() == nil && v != binary.LittleEndian.Uint32(data[prevOff:]) {
					t.Fatalf("uint32 at %d = %#x", prevOff, v)
				}
			case 5:
				v := d.Uint32BE("u32be")
				if d.Err() == nil && v != binary.BigEndian.Uint32(data[prevOff:]) {
					t.Fatalf("uint32be at %d = %#x", prevOff, v)
				}
			case 6:
				v := d.Uint64("u64")
				if d.Err() == nil && v != binary.LittleEndian.Uint64(data[prevOff:]) {
					t.Fatalf("uint64 at %d = %#x", prevOff, v)
				}
			case 7:
				at := operand(sel, len(data)-prevOff, len(data))
				v := d.Uint32At("u32at", at)
				if d.Err() == nil && (v != binary.LittleEndian.Uint32(data[at:]) || d.Offset() != prevOff) {
					t.Fatalf("uint32at(%d) = %#x, offset %d", at, v, d.Offset())
				}
			}

			if d.Offset() < 0 || d.Offset() > len(data) || d.Remaining() != len(data)-d.Offset() {
				t.Fatalf("offset %d out of [0, %d], remaining %d", d.Offset(), len(data), d.Remaining())
			}
			if prevErr != nil && (d.Err() != prevErr || d.Offset() != prevOff) {
				t.Fatalf("error not latched: %v -> %v, offset %d -> %d", prevErr, d.Err(), prevOff, d.Offset())
			}
			if d.Err() != nil && prevErr == nil && d.Offset() != prevOff {
				t.Fatalf("failed read moved offset %d -> %d", prevOff, d.Offset())
			}
		}
	})
}
