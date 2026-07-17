package sst

import (
	"fmt"
	"io"
	"time"

	"github.com/yashgorana/quxdb/pkg/bloom"
	"github.com/yashgorana/quxdb/pkg/fs"
)

type MappedFilter struct {
	bloom.Reader
	mmap []byte
}

func (mi *MappedFilter) Close() error {
	err := fs.Munmap(mi.mmap)
	mi.mmap = nil
	return err
}

func OpenFilter(path string) (*MappedFilter, error) {
	mmapBytes, err := mmapRead(path, fs.MADV_RANDOM)
	if err != nil {
		return nil, err
	}

	// decoded header
	_, n, err := decodeHeader(mmapBytes[0:], sstTypeFilter)
	if err != nil {
		_ = fs.Munmap(mmapBytes)
		return nil, fmt.Errorf("sparse index decode %w", err)
	}

	var filter bloom.BloomFilter
	err = filter.UnmarshalSlice(mmapBytes[n:])
	if err != nil {
		_ = fs.Munmap(mmapBytes)
		return nil, err
	}

	return &MappedFilter{
		Reader: &filter,
		mmap:   mmapBytes,
	}, nil
}

func WriteFilter(w io.Writer, filter *bloom.BloomFilter) (int, error) {
	// write header
	h := sstHeader{sstTypeFilter, sstVersion, time.Now()}
	hn, err := writeHeader(w, h)
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	// write data
	buf, err := filter.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("filter encode %w", err)
	}
	dn, err := w.Write(buf)
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	return hn + dn, nil
}
