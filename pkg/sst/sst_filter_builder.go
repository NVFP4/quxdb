package sst

import (
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

func newFilterWriter(dir string, id uint64, expectedKeys uint64) *filterBuilder {
	return &filterBuilder{
		path:   filepath.Join(dir, sstFilterName(id)),
		filter: bloom.NewWithBitsPerKey(expectedKeys, filterBitsPerKey),
	}
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

	if _, err = WriteFilter(fd, fb.filter); err != nil {
		return err
	}
	return fs.SyncData(fd)
}

func (fb *filterBuilder) Close() error {
	fb.filter = nil
	return nil
}
