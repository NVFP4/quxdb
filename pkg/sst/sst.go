package sst

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/pathlib"
	"golang.org/x/sync/errgroup"
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

var (
	ErrInvalidFormat      = errors.New("sst: invalid format")
	ErrUnsupportedVersion = errors.New("sst: unsupported version")
	ErrCorrupt            = errors.New("sst: corrupt")
	ErrChecksumMismatch   = errors.New("sst: checksum mismatch")
)

// FileHashes holds the sha256 of each table file.
type FileHashes struct {
	Data   string `json:"qdat"`
	Index  string `json:"qidx"`
	Filter string `json:"qfltr"`
}

// Metadata describes a table, immutable and shared by pointer.
type Metadata struct {
	Version    uint16     `json:"ver"`
	ID         uint64     `json:"id"`
	Level      uint8      `json:"lvl"`
	Path       string     `json:"path"`
	CreatedAt  time.Time  `json:"ts"`
	MinKey     []byte     `json:"minKey"`
	MaxKey     []byte     `json:"maxKey"`
	Keys       uint64     `json:"keys,omitempty"`
	SizeBytes  uint64     `json:"sizeBytes,omitempty"`
	FileHashes FileHashes `json:"sha256"`
}

// Span is a byte range in a table file.
type Span struct {
	Offset int
	Size   int
}

// SST is an open, memory-mapped table.
type SST struct {
	data   *MappedBlockData
	index  *MappedSparseIndex
	filter *MappedFilter
}

func openSST(meta *Metadata) (*SST, error) {
	var (
		data   *MappedBlockData
		index  *MappedSparseIndex
		filter *MappedFilter
		group  errgroup.Group
	)

	if !pathlib.DirExists(meta.Path) {
		return nil, fmt.Errorf("path not found '%s'", meta.Path)
	}

	group.Go(func() error {
		var err error
		path := filepath.Join(meta.Path, sstBlockDataName(meta.ID))
		data, err = OpenBlockData(path)
		return err
	})

	group.Go(func() error {
		var err error
		path := filepath.Join(meta.Path, sstIndexName(meta.ID))
		index, err = OpenSparseIndex(path)
		return err
	})

	group.Go(func() error {
		var err error
		path := filepath.Join(meta.Path, sstFilterName(meta.ID))
		filter, err = OpenFilter(path)
		return err
	})

	if err := group.Wait(); err != nil {
		var errs []error
		errs = append(errs, err)
		if data != nil {
			errs = append(errs, data.Close())
		}
		if index != nil {
			errs = append(errs, index.Close())
		}
		if filter != nil {
			errs = append(errs, filter.Close())
		}
		return nil, fmt.Errorf("sst open: %s %w", meta.Path, errors.Join(errs...))
	}

	return &SST{
		data:   data,
		index:  index,
		filter: filter,
	}, nil
}

func (s *SST) close() error {
	return errors.Join(s.data.Close(), s.index.Close(), s.filter.Close())
}

func mmapRead(path string, advice fs.MmapAdvice) ([]byte, error) {
	fd, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("file read %w", err)
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat %w", err)
	}

	mmapBytes, err := fs.Mmap(fd, 0, info.Size())
	if err != nil {
		return nil, fmt.Errorf("file mmap %w", err)
	}

	if err := fs.Madvice(mmapBytes, advice); err != nil {
		_ = fs.Munmap(mmapBytes)
		return nil, err
	}

	return mmapBytes, nil
}

// fixed width keeps names sorted by id.
func sstName(id uint64) string {
	return fmt.Sprintf("%020d", id)
}

func sstBlockDataName(id uint64) string {
	return sstName(id) + ".qdat"
}

func sstIndexName(id uint64) string {
	return sstName(id) + ".qidx"
}

func sstFilterName(id uint64) string {
	return sstName(id) + ".qfltr"
}

// flat layout, the level lives in the catalog
func sstDirPath(baseDir string, id uint64) string {
	return filepath.Join(baseDir, "sst", sstName(id))
}
