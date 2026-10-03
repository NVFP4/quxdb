package sst

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
)

const (
	blockSizeTarget       = 32 << 10 // ~32 KiB Data
	blockIndexStrideBytes = 2 << 10  // index key every 2 KiB
	blockIndexStrideKeys  = 8        // index key every 8 keys
	blockIndexCap         = 256      // yolo value
)

// range of bytes within a block
type BlockSpan Span

type blockBuilder struct {
	fd       *os.File // .qdat file
	fdOff    int      // offset in .qdat
	writeBuf []byte   // write buffer for `fd`
	written  int

	blockData  Block
	blockState blockState
	indexState indexState
}

func newBlockWriter(dir string, id uint64, dataSizeBytes uint64) (*blockBuilder, error) {
	path := filepath.Join(dir, sstBlockDataName(id))
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	fileSize := int(dataSizeBytes + dataSizeBytes>>4)
	fileSize = alignUpPage(fileSize)
	if err := fs.Fallocate(fd, 0, int64(fileSize)); err != nil {
		return nil, err
	}

	return &blockBuilder{
		fd:        fd,
		writeBuf:  make([]byte, 0, alignUpPage(blockSizeTarget)),
		blockData: newBlock(blockIndexCap),
	}, nil
}

func (bb *blockBuilder) Add(key, val []byte) (*blockState, error) {
	var lastBlock *blockState

	n := bb.canFit(key, val)
	if n < 0 {
		// cannot fit, flush the current block
		b, err := bb.writeBlock(false)
		if err != nil {
			return nil, err
		}
		lastBlock = b
	}

	recSpan := bb.addBlockRecord(key, val)

	if bb.indexState.startKey == nil {
		bb.indexState.startKey = key
		bb.indexState.startKeyOffset = recSpan.Offset
	}

	if bb.shouldIndexRecord() {
		bb.indexRecord(key)
	}

	return lastBlock, nil
}

func (bb *blockBuilder) Finalize() (state *blockState, err error) {
	if bb.hasKeys() {
		state, err = bb.writeBlock(true)
		if err != nil {
			return nil, err
		}
	}

	return state, fs.Fdatasync(bb.fd)
}

func (bb *blockBuilder) Close() error {
	err := errors.Join(
		bb.fd.Truncate(int64(bb.fdOff)),
		bb.fd.Close(),
	)

	bb.reset()
	bb.fd = nil
	return err
}

func (bb *blockBuilder) canFit(key, val []byte) int {
	kvSize := len(key) + len(val)

	// k/v is bigger than block size
	if kvSize > blockSizeTarget {
		return -2
	}

	// kv will overflow the block
	blockSize := bb.blockState.fileSpan.Size + int(bb.blockData.index.SizeBytes()) + len(key)
	if blockSize+kvSize > blockSizeTarget {
		return -1
	}

	return 0
}

func (bb *blockBuilder) addBlockRecord(key, val []byte) BlockSpan {
	if bb.blockState.keys == 0 {
		bb.blockState.minKey = key
		// reserve space for header
		bb.writeBuf = bb.writeBuf[:blockHeaderLen]
		bb.blockState.fileSpan.Size = len(bb.writeBuf)
	}

	recOff := len(bb.writeBuf)
	bb.writeBuf = encodeBlockRecord(bb.writeBuf, key, val)
	n := len(bb.writeBuf) - recOff
	bb.blockState.keys += 1
	bb.blockState.fileSpan.Size += n
	bb.blockState.maxKey = key

	return BlockSpan{recOff, n}
}

func (bb *blockBuilder) writeBlock(isLastBlock bool) (*blockState, error) {
	// block is already sealed, you need to reset it
	if bb.blockState.sealed {
		return nil, fmt.Errorf("block is already sealed and flushed")
	}

	// add the last key to index. start == nil if we already added the last key
	if bb.indexState.startKey != nil {
		bb.indexRecord(bb.blockState.maxKey)
	}

	bb.writeBuf = encodeBlock(bb.writeBuf, &bb.blockData)
	bb.blockState.fileSpan.Size = len(bb.writeBuf) // final size of the block

	if !isLastBlock {
		// align up to nearest 8 byte boundary
		pre := len(bb.writeBuf)
		bb.writeBuf = bb.writeBuf[:alignUpPage(len(bb.writeBuf))]
		// zero out the padding!
		clear(bb.writeBuf[pre:])
	} else {
		var buf [sstHeaderLen]byte
		fm := sstHeader{sstTypeData, sstVersion, time.Now()}
		_ = encodeHeader(buf[:], fm)
		bb.writeBuf = append(bb.writeBuf, buf[:]...)
	}

	bb.blockState.sealed = true

	// write file to disk
	nn, err := bb.fd.Write(bb.writeBuf)
	if err != nil {
		return nil, err
	}
	if nn != len(bb.writeBuf) {
		return nil, io.ErrShortWrite
	}

	// decoupling block span with file span, because blocks will be compressed
	blockState := bb.blockState
	blockState.fileSpan = Span{bb.fdOff, blockState.fileSpan.Size}
	bb.fdOff += nn

	bb.reset()

	return &blockState, nil
}

func (bb *blockBuilder) shouldIndexRecord() bool {
	keysSinceIndex := bb.blockState.keys - bb.indexState.keys
	bytesSinceIndex := bb.blockState.fileSpan.Size - bb.indexState.size

	should := keysSinceIndex >= blockIndexStrideKeys ||
		bytesSinceIndex >= blockIndexStrideBytes

	return should
}

func (bb *blockBuilder) indexRecord(key []byte) {
	bb.blockData.index.add(bb.indexState.startKey, key, bb.indexState.startKeyOffset)
	bb.indexState.startKey = nil
	bb.indexState.keys = bb.blockState.keys
	bb.indexState.size = bb.blockState.fileSpan.Size
}

func (bb *blockBuilder) hasKeys() bool {
	return bb.blockState.keys > 0 && !bb.blockState.sealed
}

func (bb *blockBuilder) reset() {
	bb.blockState.reset()
	bb.indexState.reset()
	bb.blockData.clear()
	clear(bb.writeBuf)
	bb.writeBuf = bb.writeBuf[:0]
}

func alignUpPage(n int) int {
	return (n + 4095) &^ 4095
}

// -----

type blockState struct {
	keys     int
	minKey   []byte
	maxKey   []byte
	fileSpan Span
	sealed   bool
}

func (b *blockState) reset() {
	b.keys = 0
	b.minKey = nil
	b.maxKey = nil
	b.fileSpan = Span{}
	b.sealed = false
}

type indexState struct {
	startKey       []byte
	startKeyOffset int
	keys           int
	size           int
}

func (is *indexState) reset() {
	is.startKey = nil
	is.startKeyOffset = 0
	is.keys = 0
	is.size = 0
}
