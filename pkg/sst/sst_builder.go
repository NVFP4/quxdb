package sst

import (
	"bytes"
	"errors"
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
	ErrEmptyBuild       = errors.New("sst: empty build")
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
	Keys      uint64 // expected keys, sizes the bloom filter
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

// NewBuilder starts building table opts.ID with DefaultOptions changed by options.
func NewBuilder(opts BuilderOpts, options ...Option) (*Builder, error) {
	layout := DefaultOptions()
	for _, opt := range options {
		opt(&layout)
	}
	return NewBuilderWithOptions(opts, layout)
}

// NewBuilderWithOptions starts building table opts.ID laid out by layout.
func NewBuilderWithOptions(opts BuilderOpts, layout Options) (*Builder, error) {
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

	bw, err := newBlockWriter(sstDir, id, opts.SizeBytes, layout)
	if err != nil {
		return nil, errors.Join(
			err,
			os.RemoveAll(sstDir),
		)
	}

	iw, err := newIndexWriter(sstDir, id, opts.SizeBytes, layout.BlockTargetBytes)
	if err != nil {
		return nil, errors.Join(
			err,
			bw.Close(),
			os.RemoveAll(sstDir),
		)
	}

	fw := newFilterWriter(sstDir, id, opts.Keys, layout.FilterBitsPerKey)

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
	if b.keys == 0 {
		return nil, ErrEmptyBuild
	}

	b.closed = true

	finalizeErr := b.finalizeAll()
	sizeBytes := b.blockWriter.fdOff
	if err := errors.Join(finalizeErr, b.closeAll()); err != nil {
		return nil, err
	}

	// persist the table files' entries, then the rename of their dir
	if err := fs.SyncDir(b.sstDir); err != nil {
		return nil, err
	}
	sstFinalPath := strings.TrimSuffix(b.sstDir, ".tmp")
	if err := fs.RenameDurable(b.sstDir, sstFinalPath); err != nil {
		return nil, err
	}

	metadata := &Metadata{
		Version:   footerVersion,
		ID:        b.id,
		Path:      sstFinalPath,
		Level:     b.level,
		MinKey:    bytes.Clone(b.sstMinKey),
		MaxKey:    bytes.Clone(b.sstMaxKey),
		Keys:      b.keys,
		SizeBytes: uint64(sizeBytes),
		CreatedAt: time.Now().UTC(),
	}

	b.sstMinKey = nil
	b.sstMaxKey = nil

	return metadata, nil
}

// Abort discards the build.
func (b *Builder) Abort() error {
	b.closed = true

	// writers are already closed after a failed Finalize
	var err error
	if b.blockWriter != nil {
		err = b.closeAll()
	}
	return errors.Join(err, os.RemoveAll(b.sstDir))
}

func (b *Builder) finalizeAll() error {
	block, err := b.blockWriter.Finalize()
	if err != nil {
		return err
	}
	if block != nil {
		if err := b.indexWriter.Add(block); err != nil {
			return err
		}
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
	err := errors.Join(b.blockWriter.Close(), b.indexWriter.Close(), b.filterWriter.Close())
	b.blockWriter = nil
	b.indexWriter = nil
	b.filterWriter = nil
	return err
}
