package sst

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
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
	ErrTableRetired       = errors.New("sst: table retired")
)

type FileHashes struct {
	Data   string `json:"qdat"`
	Index  string `json:"qidx"`
	Filter string `json:"qfltr"`
}

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

// range of bytes
type Span struct {
	Offset int
	Size   int
}

type SST struct {
	meta   Metadata
	data   *MappedBlockData
	index  *MappedSparseIndex
	filter *MappedFilter
}

func openSST(meta Metadata) (*SST, error) {
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
		meta:   meta,
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

func sstBlockDataName(id uint64) string {
	return strconv.FormatUint(id, 10) + ".qdat"
}

func sstIndexName(id uint64) string {
	return strconv.FormatUint(id, 10) + ".qidx"
}

func sstFilterName(id uint64) string {
	return strconv.FormatUint(id, 10) + ".qfltr"
}

func sstDirPath(baseDir string, level uint8, id uint64) string {
	return filepath.Join(sstLevelPath(baseDir, level), strconv.FormatUint(id, 10))
}

func sstLevelPath(baseDir string, level uint8) string {
	return filepath.Join(baseDir, fmt.Sprintf("L%d", level))
}
