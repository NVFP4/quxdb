package sst

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

// Metadata describes a table, immutable and shared by pointer.
type Metadata struct {
	Version   uint16    `json:"ver"`
	ID        uint64    `json:"id"`
	Level     uint8     `json:"lvl"`
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"ts"`
	MinKey    []byte    `json:"minKey"`
	MaxKey    []byte    `json:"maxKey"`
	Keys      uint64    `json:"keys,omitempty"`
	SizeBytes uint64    `json:"sizeBytes,omitempty"`
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

const (
	tablesDir      = "sst"
	tableNameWidth = 20 // digits in max uint64
)

// fixed width keeps names sorted by id.
func sstName(id uint64) string {
	return fmt.Sprintf("%0*d", tableNameWidth, id)
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
	return filepath.Join(baseDir, tablesDir, sstName(id))
}

// RemoveOrphans deletes table dirs not in live, call only while no builder runs.
func RemoveOrphans(baseDir string, live []*Metadata, log *slog.Logger) error {
	dir := filepath.Join(baseDir, tablesDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	ids := make(map[uint64]struct{}, len(live))
	for _, meta := range live {
		ids[meta.ID] = struct{}{}
	}

	var errs []error
	for _, entry := range entries {
		name, tmp := strings.CutSuffix(entry.Name(), ".tmp")
		if len(name) != tableNameWidth {
			continue
		}
		id, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		if _, isLive := ids[id]; isLive && !tmp {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, err)
			continue
		}
		log.Info("removed orphan table", "path", path)
	}
	return errors.Join(errs...)
}
