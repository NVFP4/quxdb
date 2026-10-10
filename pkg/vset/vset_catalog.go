package vset

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/jsonl"
	"github.com/yashgorana/quxdb/pkg/sst"
)

const (
	catalogFilename   = "QUXCATALOG"
	catalogBufferSize = 4 << 10
)

var (
	ErrClosed        = errors.New("vset: closed")
	ErrRecordInvalid = errors.New("catalog: record invalid format")
	ErrRecordCorrupt = errors.New("catalog: record corrupted")
)

type Op int

const (
	OpAdd Op = iota + 1
	OpDelete
	OpCheckpoint
)

type catalogRecord struct {
	Op         Op            `json:"op"`
	Table      *sst.Metadata `json:"table,omitempty"`
	Checkpoint *Checkpoint   `json:"checkpoint,omitempty"`
}

type catalog struct {
	path    string
	fd      *os.File
	bw      *bufio.Writer
	jw      jsonl.Writer
	records int
}

func openCatalog(dir string) (*catalog, error) {
	path := filepath.Join(dir, catalogFilename)

	// cleanup stale partial catalog
	if err := os.Remove(path + ".tmp"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("vset: %w", err)
	}

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("vset: %w", err)
	}

	bw := bufio.NewWriterSize(fd, catalogBufferSize)
	jw := jsonl.NewWriter(bw)

	return &catalog{
		path: path,
		fd:   fd,
		bw:   bw,
		jw:   jw,
	}, nil
}

// replay feeds callback every record and returns the bytes of a torn tail it cut off.
func (c *catalog) replay(callback func(catalogRecord) error) (int64, error) {
	if _, err := c.fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	jr := jsonl.NewReader[catalogRecord](c.fd)

	for rec, err := range jr.Records() {
		if err != nil {
			if !errors.Is(err, jsonl.ErrTornRead) {
				return 0, err
			}
			size, err := c.size()
			if err != nil {
				return 0, err
			}
			safe := int64(jr.SafeOffset())
			return size - safe, c.truncate(safe)
		}
		if err := callback(rec); err != nil {
			return 0, err
		}
		c.records++
	}

	return 0, nil
}

func (c *catalog) appendAll(records []catalogRecord) error {
	start, err := c.size()
	if err != nil {
		return err
	}

	for _, rec := range records {
		if _, err := c.jw.Write(rec); err != nil {
			return errors.Join(err, c.truncate(start))
		}
	}

	if err := c.bw.Flush(); err != nil {
		return errors.Join(err, c.truncate(start))
	}

	if err := c.sync(); err != nil {
		return errors.Join(err, c.truncate(start))
	}

	c.records += len(records)
	return nil
}

// atomically swaps in a catalog of records and appends to it from then on.
func (c *catalog) rewrite(records []catalogRecord) error {
	tmpPath := c.path + ".tmp"
	fd, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(fd, catalogBufferSize)
	jw := jsonl.NewWriter(bw)

	write := func() error {
		for _, rec := range records {
			if _, err := jw.Write(rec); err != nil {
				return err
			}
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		if err := fs.SyncData(fd); err != nil {
			return err
		}
		return os.Rename(tmpPath, c.path)
	}
	if err := write(); err != nil {
		return errors.Join(err, fd.Close(), os.Remove(tmpPath))
	}

	// the new file is the catalog now, never fall back
	old := c.fd
	c.fd, c.bw, c.jw, c.records = fd, bw, jw, len(records)
	return errors.Join(old.Close(), fs.SyncDir(filepath.Dir(c.path)))
}

func (c *catalog) size() (int64, error) {
	st, err := c.fd.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (c *catalog) truncate(size int64) error {
	c.bw.Reset(c.fd)
	if err := c.fd.Truncate(size); err != nil {
		return err
	}
	if _, err := c.fd.Seek(size, io.SeekStart); err != nil {
		return err
	}
	return c.sync()
}

func (c *catalog) sync() error {
	return fs.SyncData(c.fd)
}

func (c *catalog) close() error {
	return c.fd.Close()
}
