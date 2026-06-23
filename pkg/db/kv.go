package db

import (
	"encoding/binary"
)

type quxKV struct {
	qkey quxKey
	val  []byte
}

func (r *quxKV) EncodedLen() int {
	// 4 for lenKey, 4 for lenVal
	return 8 + len(r.qkey) + len(r.val)
}

func (r *quxKV) Encode(buf []byte) int {
	bufOff := 0
	lenKey := len(r.qkey)
	lenVal := len(r.val)

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenKey))
	bufOff += 4

	copy(buf[bufOff:], r.qkey)
	bufOff += lenKey

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenVal))
	bufOff += 4

	copy(buf[bufOff:], r.val)
	bufOff += lenVal

	return bufOff
}

func (r *quxKV) Decode(buf []byte) int {
	bufOff := 0

	lenKey := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.qkey = buf[bufOff : bufOff+int(lenKey)]
	bufOff += int(lenKey)

	lenVal := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.val = buf[bufOff : bufOff+int(lenVal)]
	bufOff += int(lenVal)

	return bufOff
}
