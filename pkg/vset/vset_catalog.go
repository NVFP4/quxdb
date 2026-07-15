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
)

const (
	catFilename       = "QUXCATALOG"
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
)

type catalogRecord struct {
	Op   Op        `json:"op"`
	Meta TableMeta `json:"meta"`
}

type catalog struct {
	path string
	fd   *os.File
	bw   *bufio.Writer
	jw   jsonl.Writer
}

func openCatalog(dir string) (*catalog, error) {
	path := filepath.Join(dir, catFilename)

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

func (c *catalog) replay(callback func(catalogRecord) error) error {
	if _, err := c.fd.Seek(0, io.SeekStart); err != nil {
		return err
	}

	jr := jsonl.NewReader[catalogRecord](c.fd)

	for rec, err := range jr.Records() {
		if err != nil {
			if errors.Is(err, jsonl.ErrTornRead) {
				return c.truncate(int64(jr.SafeOffset()))
			}
			return err
		}
		if err := callback(rec); err != nil {
			return err
		}
	}

	return nil
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

	return nil
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
	return fs.Fdatasync(c.fd)
}

func (c *catalog) close() error {
	return c.fd.Close()
}
