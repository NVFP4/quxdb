package codec

import (
	"encoding/binary"
	"fmt"
)

// latched Decoder
type Decoder struct {
	data []byte
	off  int
	err  error
}

func NewDecoder(data []byte) *Decoder {
	return &Decoder{
		data: data,
	}
}

func (d *Decoder) Err() error {
	return d.err
}

func (d *Decoder) Offset() int {
	return d.off
}

func (d *Decoder) Remaining() int {
	return len(d.data) - d.off
}

func (d *Decoder) Uint16(field string) uint16 {
	b := d.take(field, 2)
	if b == nil {
		return 0
	}

	return binary.LittleEndian.Uint16(b)
}

func (d *Decoder) Uint16BE(field string) uint16 {
	b := d.take(field, 2)
	if b == nil {
		return 0
	}

	return binary.BigEndian.Uint16(b)
}

func (d *Decoder) Uint32(field string) uint32 {
	b := d.take(field, 4)
	if b == nil {
		return 0
	}

	return binary.LittleEndian.Uint32(b)
}

func (d *Decoder) Uint32At(field string, offset int) uint32 {
	if d.err != nil {
		return 0
	}

	if offset+4 > len(d.data) {
		d.err = fmt.Errorf(
			"decode %q at offset %d: need 4 bytes, have %d",
			field,
			offset,
			len(d.data)-offset, // negative is fine
		)
		return 0
	}

	return binary.LittleEndian.Uint32(d.data[offset : offset+4])
}

func (d *Decoder) Uint32BE(field string) uint32 {
	b := d.take(field, 4)
	if b == nil {
		return 0
	}

	return binary.BigEndian.Uint32(b)
}

func (d *Decoder) Uint64(field string) uint64 {
	b := d.take(field, 8)
	if b == nil {
		return 0
	}

	return binary.LittleEndian.Uint64(b)
}

func (d *Decoder) UVarint(field string) uint64 {
	if d.err != nil {
		return 0
	}

	value, n := binary.Uvarint(d.data[d.off:])
	switch {
	case n > 0:
		d.off += n
		return value

	case n == 0:
		d.err = fmt.Errorf(
			"decode %q at offset %d: truncated uvarint",
			field,
			d.off,
		)

	default:
		d.err = fmt.Errorf(
			"decode %q at offset %d: uvarint overflow",
			field,
			d.off,
		)
	}

	return 0
}

func (d *Decoder) Bytes(field string, n int) []byte {
	return d.take(field, n)
}

func (d *Decoder) take(field string, n int) []byte {
	if d.err != nil {
		return nil
	}

	if n < 0 {
		d.err = fmt.Errorf(
			"decode %q at offset %d: negative length %d",
			field,
			d.off,
			n,
		)
		return nil
	}

	if d.off+n > len(d.data) {
		d.err = fmt.Errorf(
			"decode %q at offset %d: need %d bytes, have %d",
			field,
			d.off,
			n,
			len(d.data)-d.off,
		)
		return nil
	}

	b := d.data[d.off : d.off+n]
	d.off += n
	return b
}

func (d *Decoder) fail(field string, offset int, err error) {
	if d.err != nil {
		return
	}

	if field == "" {
		d.err = fmt.Errorf("decode at offset %d: %w", offset, err)
		return
	}

	d.err = fmt.Errorf("decode %q at offset %d: %w", field, offset, err)
}
