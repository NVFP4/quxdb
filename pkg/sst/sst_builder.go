package sst

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/pathlib"
)

var (
	ErrEmptyKey         = errors.New("sst: empty key")
	ErrClosed           = errors.New("sst: already closed")
	ErrAlreadyFinalized = errors.New("sst: already finalized")
)

// Record is one table entry.
type Record struct {
	OrderedKey []byte
	FilterKey  []byte
	Value      []byte
}

// BuilderOpts configures a table build.
type BuilderOpts struct {
	Dir       string
	ID        uint64
	Level     uint8
	Keys      uint64
	SizeBytes uint64
}

// Builder writes a table to a tmp dir and publishes it on Finalize.
type Builder struct {
	id     uint64
	sstDir string
	level  uint8

	blockWriter  *blockBuilder
	indexWriter  *indexBuilder
	filterWriter *filterBuilder

	sstMinKey []byte
	sstMaxKey []byte
	keys      uint64

	closed bool
}

// NewBuilder starts building table opts.ID.
func NewBuilder(opts BuilderOpts) (*Builder, error) {
	id := opts.ID
	finalDir := sstDirPath(opts.Dir, id)
	sstDir := finalDir + ".tmp"

	// clear leftovers of an uncommitted build with this id
	if err := errors.Join(os.RemoveAll(finalDir), os.RemoveAll(sstDir)); err != nil {
		return nil, err
	}
	if err := pathlib.EnsureDir(sstDir); err != nil {
		return nil, err
	}

	bw, err := newBlockWriter(sstDir, id, opts.SizeBytes)
	if err != nil {
		return nil, errors.Join(
			err,
			os.RemoveAll(sstDir),
		)
	}

	iw, err := newIndexWriter(sstDir, id, opts.SizeBytes)
	if err != nil {
		return nil, errors.Join(
			err,
			bw.Close(),
			os.RemoveAll(sstDir),
		)
	}

	fw, err := newFilterWriter(sstDir, id, opts.Keys)
	if err != nil {
		return nil, errors.Join(
			err,
			bw.Close(),
			iw.Close(),
			os.RemoveAll(sstDir),
		)
	}

	return &Builder{
		id:           id,
		sstDir:       sstDir,
		blockWriter:  bw,
		indexWriter:  iw,
		filterWriter: fw,
		level:        opts.Level,
		closed:       false,
	}, nil
}

// Add appends a record to the table.
func (b *Builder) Add(r Record) error {
	if b.closed {
		return ErrClosed
	}

	if len(r.OrderedKey) == 0 || len(r.FilterKey) == 0 {
		return ErrEmptyKey
	}

	b.filterWriter.Add(r.FilterKey)

	block, err := b.blockWriter.Add(r.OrderedKey, r.Value)
	if err != nil {
		return err
	}

	// new block was generated
	if block != nil {
		if err := b.indexWriter.Add(block); err != nil {
			return err
		}
	}

	if b.sstMinKey == nil {
		b.sstMinKey = r.FilterKey
	}
	b.sstMaxKey = r.FilterKey
	b.keys++

	return nil
}

// Finalize writes the table and returns its metadata.
func (b *Builder) Finalize() (*Metadata, error) {
	if b.closed {
		return nil, ErrAlreadyFinalized
	}

	b.closed = true

	var errs []error
	errs = append(errs, b.finalizeAll())
	errs = append(errs, b.closeAll())
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	// rename .tmp dir to final dir
	sstFinalPath := strings.TrimSuffix(b.sstDir, ".tmp")
	if err := os.Rename(b.sstDir, sstFinalPath); err != nil {
		return nil, err
	}

	// fdatasync the dir
	final, err := os.Open(sstFinalPath)
	if err != nil {
		fmt.Printf("sst: could not sync sst dir '%s' %v", sstFinalPath, err)
	}
	_ = fs.Fdatasync(final)

	metadata := &Metadata{
		Version: sstVersion,
		ID:      b.id,
		Path:    sstFinalPath,
		Level:   b.level,
		FileHashes: FileHashes{
			Data:   hex.EncodeToString(b.blockWriter.SHA256()),
			Index:  hex.EncodeToString(b.indexWriter.SHA256()),
			Filter: hex.EncodeToString(b.filterWriter.SHA256()),
		},
		MinKey:    bytes.Clone(b.sstMinKey),
		MaxKey:    bytes.Clone(b.sstMaxKey),
		Keys:      b.keys,
		SizeBytes: uint64(b.blockWriter.fdOff),
		CreatedAt: time.Now().UTC(),
	}

	b.blockWriter = nil
	b.filterWriter = nil
	b.indexWriter = nil
	b.sstMinKey = nil
	b.sstMaxKey = nil

	return metadata, nil
}

// Abort discards the build.
func (b *Builder) Abort() error {
	b.closed = true

	if err := b.closeAll(); err != nil {
		return err
	}

	// delete the dir
	return os.RemoveAll(b.sstDir)
}

func (b *Builder) finalizeAll() error {
	block, err := b.blockWriter.Finalize()
	if err != nil {
		return err
	}
	if block != nil {
		b.indexWriter.Add(block)
	}

	if err := b.indexWriter.Finalize(); err != nil {
		return err
	}

	if err := b.filterWriter.Finalize(); err != nil {
		return err
	}

	return nil
}

func (b *Builder) closeAll() error {
	var errs []error
	errs = append(errs, b.blockWriter.Close())
	errs = append(errs, b.indexWriter.Close())
	errs = append(errs, b.filterWriter.Close())
	return errors.Join(errs...)
}
