package sst

import (
	"bufio"
	"os"
	"path/filepath"

	"github.com/yashgorana/quxdb/pkg/fs"
)

type indexBuilder struct {
	path  string
	index SparseIndex
}

func newIndexWriter(dir string, id uint64, dataSizeBytes uint64) (*indexBuilder, error) {
	path := filepath.Join(dir, sstIndexName(id))

	// 1.5x how many blocks may fit in provided data size
	cap := int(dataSizeBytes / blockSizeTarget)
	cap = cap + cap>>1

	return &indexBuilder{
		path:  path,
		index: newSparseIndex(cap),
	}, nil
}

func (ib *indexBuilder) Add(block *blockState) error {
	return ib.index.add(block.minKey, block.maxKey, block.fileSpan)
}

func (ib *indexBuilder) Finalize() (err error) {
	fd, err := os.OpenFile(ib.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer fd.Close()

	bw := bufio.NewWriterSize(fd, indexBufCap)
	_, err = WriteSparseIndex(bw, &ib.index)
	if err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return err
	}

	err = fs.Fdatasync(fd)
	return err
}

func (ib *indexBuilder) Close() error {
	ib.index.clear()
	return nil
}
