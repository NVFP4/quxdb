package db

import (
	"encoding/binary"
	"math/bits"
)

type quxKV struct {
	qkey quxKey
	val  []byte
}

func (r *quxKV) EncodedLen() int {
	lenKey := len(r.qkey)
	lenVal := len(r.val)
	return sizeUvarint(lenKey) + sizeUvarint(lenVal) + lenKey + lenVal
}

func (r *quxKV) Encode(buf []byte) int {
	bufOff := 0
	lenKey := len(r.qkey)
	lenVal := len(r.val)

	n := binary.PutUvarint(buf[bufOff:], uint64(lenKey))
	bufOff += n

	copy(buf[bufOff:], r.qkey)
	bufOff += lenKey

	n = binary.PutUvarint(buf[bufOff:], uint64(lenVal))
	bufOff += n

	copy(buf[bufOff:], r.val)
	bufOff += lenVal

	return bufOff
}

func (r *quxKV) Decode(buf []byte) int {
	bufOff := 0

	lenKey, n := binary.Uvarint(buf[bufOff:])
	bufOff += n

	r.qkey = buf[bufOff : bufOff+int(lenKey)]
	bufOff += int(lenKey)

	lenVal, n := binary.Uvarint(buf[bufOff:])
	bufOff += n

	r.val = buf[bufOff : bufOff+int(lenVal)]
	bufOff += int(lenVal)

	return bufOff
}

func sizeUvarint(x int) int {
	return (bits.Len64(uint64(x)|1) + 6) / 7
}
