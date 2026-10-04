package sst

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
	err := fs.Unmap(mi.mmap)
	mi.mmap = nil
	return err
}

func OpenFilter(path string) (*MappedFilter, error) {
	mmapBytes, err := fs.MapFile(path, fs.AdviceRandom)
	if err != nil {
		return nil, err
	}

	_, n, err := decodeHeader(mmapBytes, sstTypeFilter)
	if err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("filter decode %w", err)
	}

	crcOff := len(mmapBytes) - 4
	if crcOff < n || binary.LittleEndian.Uint32(mmapBytes[crcOff:]) != crc32.Checksum(mmapBytes[:crcOff], crc32Table) {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("filter decode %w", ErrChecksumMismatch)
	}

	var filter bloom.BloomFilter
	err = filter.UnmarshalSlice(mmapBytes[n:crcOff])
	if err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, err
	}

	return &MappedFilter{
		Reader: &filter,
		mmap:   mmapBytes,
	}, nil
}

func WriteFilter(w io.Writer, filter *bloom.BloomFilter) (int, error) {
	crc := crc32.New(crc32Table)
	mw := io.MultiWriter(w, crc)

	hn, err := writeHeader(mw, sstHeader{sstTypeFilter, sstVersion, time.Now()})
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	data, err := filter.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("filter encode %w", err)
	}
	dn, err := mw.Write(data)
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	cn, err := w.Write(binary.LittleEndian.AppendUint32(nil, crc.Sum32()))
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}
	return hn + dn + cn, nil
}
