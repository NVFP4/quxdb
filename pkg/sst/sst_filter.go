package sst

/*

SST FILTER FILE (.qfltr)
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
bloom		var		Bloom Filter binary data
crc			4		CRC32C of the bloom data
footer		18		QFLT File Footer (see sst_footer.go)
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`

*/

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/yashgorana/quxdb/pkg/bloom"
	"github.com/yashgorana/quxdb/pkg/fs"
)

// bloom filter aliasing a mapped filter file
type MappedFilter struct {
	bloom.BloomFilter
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

	if _, err := decodeFooter(mmapBytes, fileTypeFilter); err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("filter decode %w", err)
	}

	crcOff := len(mmapBytes) - footerLen - 4
	if crcOff < 0 || binary.LittleEndian.Uint32(mmapBytes[crcOff:]) != crc32.Checksum(mmapBytes[:crcOff], crc32Table) {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("filter decode %w", ErrChecksumMismatch)
	}

	mf := &MappedFilter{mmap: mmapBytes}
	err = mf.UnmarshalSlice(mmapBytes[:crcOff])
	if err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, err
	}

	return mf, nil
}

func WriteFilter(w io.Writer, filter *bloom.BloomFilter) (int, error) {
	data, err := filter.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("filter encode %w", err)
	}
	dn, err := w.Write(data)
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	cn, err := w.Write(binary.LittleEndian.AppendUint32(nil, crc32.Checksum(data, crc32Table)))
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}

	fn, err := writeFooter(w, sstFooter{fileTypeFilter, footerVersion, time.Now()})
	if err != nil {
		return 0, fmt.Errorf("filter write %w", err)
	}
	return dn + cn + fn, nil
}
