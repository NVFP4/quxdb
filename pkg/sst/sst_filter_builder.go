package sst

/*

SST FILTER FILE (.qdat)
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
header		18		QFLT File Header (see sst_header.go)
bloom		var		Bloom Filter binary data
crc			4		CRC32C of all preceding fields
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`

*/

import (
	"bufio"
	"os"
	"path/filepath"

	"github.com/yashgorana/quxdb/pkg/bloom"
	"github.com/yashgorana/quxdb/pkg/fs"
)

const (
	filterBitsPerKey = 12
)

type filterBuilder struct {
	path   string
	filter *bloom.BloomFilter
}

func newFilterWriter(dir string, id uint64, expectedKeys uint64) (*filterBuilder, error) {
	path := filepath.Join(dir, sstFilterName(id))

	return &filterBuilder{
		path:   path,
		filter: bloom.NewWithBitsPerKey(uint64(expectedKeys), filterBitsPerKey),
	}, nil
}

func (fb *filterBuilder) Add(key []byte) {
	fb.filter.Add(key)
}

func (fb *filterBuilder) Finalize() error {
	fd, err := os.OpenFile(fb.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer fd.Close()

	bw := bufio.NewWriterSize(fd, indexBufCap)
	_, err = WriteFilter(bw, fb.filter)
	if err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}

	err = fs.Fdatasync(fd)
	return err
}

func (fb *filterBuilder) Close() error {
	fb.filter = nil
	return nil
}
