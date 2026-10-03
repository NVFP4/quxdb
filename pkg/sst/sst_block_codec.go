package sst

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/yashgorana/quxdb/pkg/codec"
)

func encodeBlockRecord(dst, key, val []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	dst = append(dst, key...)
	dst = append(dst, val...)
	return dst
}

func decodeBlockRecord(src []byte) (recordView, error) {
	var rec recordView
	decoder := codec.NewDecoder(src)

	keyLen := decoder.UVarint("record.lenKey")
	valLen := decoder.UVarint("record.lenVal")
	rec.key = decoder.Bytes("record.key", int(keyLen))
	rec.value = decoder.Bytes("record.val", int(valLen))

	if err := decoder.Err(); err != nil {
		return recordView{}, err
	}

	rec.size = decoder.Offset()
	return rec, nil
}

func encodeBlockHeader(dst []byte, blockLen int, recLen int) []byte {
	// magic
	dst = binary.BigEndian.AppendUint32(dst, blockMagic32)
	// block len
	dst = binary.LittleEndian.AppendUint32(dst, uint32(blockLen))
	// rec len
	dst = binary.LittleEndian.AppendUint32(dst, uint32(recLen))
	return dst
}

func decodeBlock(src []byte) (Block, int, error) {
	var block Block
	decoder := codec.NewDecoder(src)

	// validate start
	magic := decoder.Uint32BE("block.magic")
	if magic != blockMagic32 {
		return block, 0, ErrInvalidFormat
	}

	// verify checksum of the payload
	end := len(src)
	crcOff := end - 4
	crc := decoder.Uint32At("block.crc", crcOff)
	if crc != crc32.Checksum(src[0:crcOff], crc32Table) {
		return block, 0, ErrChecksumMismatch
	}

	// a magic + crc validation means len safe to read
	blockLen := int(decoder.Uint32("block.blockLen"))
	recLen := int(decoder.Uint32("block.recLen"))
	_ = decoder.Bytes("block.records", recLen)

	index, _, err := decodeBlockIndex(src[decoder.Offset():])
	if err != nil {
		return block, 0, err
	}

	err = decoder.Err()
	if err != nil {
		return block, 0, err
	}

	block.recLen = recLen
	block.data = src[:blockLen]
	block.index = index
	return block, blockLen, nil
}

// the encodeBlock is kinda whacky - to avoid copying a large slice
// we start with a big buf and leave first `blockHeaderLen` bytes
// we encode records as they come in `dst[blockHeaderLen:]`
// once target size is acheived,
//  1. we write the index
//  2. then write the header
//  3. then compute the crc and append at the end
func encodeBlock(dst []byte, block *Block) []byte {
	// block.data is already written in buf (lol)

	recLen := len(dst[blockHeaderLen:])

	// append block index
	dst = encodeBlockIndex(dst, &block.index)

	blockEnd := len(dst)
	blockLen := blockEnd + 4 // block end + crc
	encodeBlockHeader(dst[:0:blockHeaderLen], blockLen, recLen)

	// full crc
	crc := crc32.Checksum(dst, crc32Table)
	dst = binary.LittleEndian.AppendUint32(dst, crc)

	return dst
}
