package quxdb

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
	"github.com/yashgorana/quxdb/pkg/wal"
)

const (
	defaultDataDir          = "./quxdata"
	defaultMaxBatchRequests = 128 // most write requests committed in one wal entry

	defaultMemtableType             = memtable.BTree
	defaultMemtableCapacityBytes    = 64 << 20
	defaultMemtableCachedImmutables = 1 // immutable memtables kept in memory to serve reads before flushing

	// truncating on mid-log wal corruption drops acknowledged writes after the damage
	defaultWALCorruptionPolicy = wal.StopOnCorruption

	defaultCompactionL0FileTarget = 4
	defaultCompactionLevelFanout  = 10
	// l1 holds a few tables so picking by overlap has a choice
	defaultCompactionL1TargetBytes = 256 << 20
	// compaction outputs split at this size, l0 tables take the memtable's size
	defaultCompactionTableTargetBytes = 64 << 20
)

// a memtable must hold the largest record or a write rolls it over forever
const memtableMinBytes = 2 * (MaxKeySize + MaxValueSize)

// Option configures a DB.
type Option func(*Options)

// Options holds every DB setting.
type Options struct {
	DataDir          string            `yaml:"dataDir"`
	MaxBatchRequests int               `yaml:"maxBatchRequests"`
	Memtable         MemtableOptions   `yaml:"memtable"`
	WAL              WALOptions        `yaml:"wal"`
	SST              sst.Options       `yaml:"sst"`
	Compaction       CompactionOptions `yaml:"compaction"`
	Catalog          vset.Options      `yaml:"catalog"`
	Logger           *slog.Logger      `yaml:"-"`
}

// MemtableOptions holds memtable settings.
type MemtableOptions struct {
	Type             memtable.MemTableType `yaml:"type"`
	CapacityBytes    int                   `yaml:"capacityBytes"`
	CachedImmutables int                   `yaml:"cachedImmutables"`
}

// WALOptions holds write-ahead log settings.
type WALOptions struct {
	wal.Options      `yaml:",inline"`
	CorruptionPolicy wal.CorruptionPolicy `yaml:"corruptionPolicy"`
}

// CompactionOptions holds level sizing settings.
type CompactionOptions struct {
	L0FileTarget     int   `yaml:"l0FileTarget"`
	LevelFanout      int   `yaml:"levelFanout"`
	L1TargetBytes    int64 `yaml:"l1TargetBytes"`
	TableTargetBytes int64 `yaml:"tableTargetBytes"`
}

// DefaultOptions returns the default DB settings.
func DefaultOptions() Options {
	return Options{
		DataDir:          defaultDataDir,
		MaxBatchRequests: defaultMaxBatchRequests,
		Memtable: MemtableOptions{
			Type:             defaultMemtableType,
			CapacityBytes:    defaultMemtableCapacityBytes,
			CachedImmutables: defaultMemtableCachedImmutables,
		},
		WAL: WALOptions{
			Options:          wal.DefaultOptions(),
			CorruptionPolicy: defaultWALCorruptionPolicy,
		},
		SST: sst.DefaultOptions(),
		Compaction: CompactionOptions{
			L0FileTarget:     defaultCompactionL0FileTarget,
			LevelFanout:      defaultCompactionLevelFanout,
			L1TargetBytes:    defaultCompactionL1TargetBytes,
			TableTargetBytes: defaultCompactionTableTargetBytes,
		},
		Catalog: vset.DefaultOptions(),
		Logger:  slog.New(slog.DiscardHandler),
	}
}

// Validate reports settings the db can't run with.
func (o Options) Validate() error {
	var errs []error
	// an empty path resolves to the working directory
	if o.DataDir == "" {
		errs = append(errs, errors.New("db: data dir is empty"))
	}
	if o.Memtable.CapacityBytes < memtableMinBytes {
		errs = append(errs, fmt.Errorf("db: memtable capacity %d below %d", o.Memtable.CapacityBytes, memtableMinBytes))
	}
	if o.Memtable.CachedImmutables < 0 {
		errs = append(errs, fmt.Errorf("db: cached immutables %d is negative", o.Memtable.CachedImmutables))
	}
	if o.MaxBatchRequests < 1 {
		errs = append(errs, fmt.Errorf("db: max batch requests %d below 1", o.MaxBatchRequests))
	}
	if o.Compaction.L0FileTarget < 1 {
		errs = append(errs, fmt.Errorf("db: l0 file target %d below 1", o.Compaction.L0FileTarget))
	}
	if o.Compaction.LevelFanout < 2 {
		errs = append(errs, fmt.Errorf("db: level fanout %d below 2", o.Compaction.LevelFanout))
	}
	if o.Compaction.L1TargetBytes <= 0 || o.Compaction.TableTargetBytes <= 0 {
		errs = append(errs, errors.New("db: compaction targets must be positive"))
	}
	if err := o.SST.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// WithDataDir sets the directory the db lives in.
func WithDataDir(dir string) Option {
	return func(o *Options) {
		o.DataDir = dir
	}
}

// WithLogger sets the logger the db and its components log to, discarded by default.
func WithLogger(logger *slog.Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// WithMemtableType sets the memtable implementation.
func WithMemtableType(t memtable.MemTableType) Option {
	return func(o *Options) {
		o.Memtable.Type = t
	}
}

// WithMemtableBytes sets the memtable capacity that triggers a rollover.
func WithMemtableBytes(size int) Option {
	return func(o *Options) {
		o.Memtable.CapacityBytes = size
	}
}

// WithCachedImmutables sets how many flushed memtables stay in memory to serve reads.
func WithCachedImmutables(n int) Option {
	return func(o *Options) {
		o.Memtable.CachedImmutables = n
	}
}

// WithWALSegmentBytes sets the size of each wal file.
func WithWALSegmentBytes(size uint64) Option {
	return func(o *Options) {
		o.WAL.SegmentBytes = size
	}
}

// WithWALReadAheadBytes sets the minimum bytes recovery reads from a wal file at once.
func WithWALReadAheadBytes(size int) Option {
	return func(o *Options) {
		o.WAL.ReadAheadBytes = size
	}
}

// WithWALCorruptionPolicy sets how recovery handles mid-log corruption.
func WithWALCorruptionPolicy(policy wal.CorruptionPolicy) Option {
	return func(o *Options) {
		o.WAL.CorruptionPolicy = policy
	}
}

// WithMaxBatchRequests sets the most write requests committed in one wal entry.
func WithMaxBatchRequests(n int) Option {
	return func(o *Options) {
		o.MaxBatchRequests = n
	}
}

// WithSSTBlockTargetBytes sets the target size of a table data block.
func WithSSTBlockTargetBytes(size int) Option {
	return func(o *Options) {
		o.SST.BlockTargetBytes = size
	}
}

// WithSSTIndexStrideBytes sets the record bytes between block index entries.
func WithSSTIndexStrideBytes(size int) Option {
	return func(o *Options) {
		o.SST.IndexStrideBytes = size
	}
}

// WithSSTIndexStrideKeys sets the records between block index entries.
func WithSSTIndexStrideKeys(keys int) Option {
	return func(o *Options) {
		o.SST.IndexStrideKeys = keys
	}
}

// WithSSTFilterBitsPerKey sets the bloom filter bits per key.
func WithSSTFilterBitsPerKey(bits uint8) Option {
	return func(o *Options) {
		o.SST.FilterBitsPerKey = bits
	}
}

// WithL0FileTarget sets the l0 table count that triggers compaction.
func WithL0FileTarget(n int) Option {
	return func(o *Options) {
		o.Compaction.L0FileTarget = n
	}
}

// WithLevelFanout sets the size ratio between adjacent levels.
func WithLevelFanout(n int) Option {
	return func(o *Options) {
		o.Compaction.LevelFanout = n
	}
}

// WithL1TargetBytes sets the l1 size target.
func WithL1TargetBytes(size int64) Option {
	return func(o *Options) {
		o.Compaction.L1TargetBytes = size
	}
}

// WithTableTargetBytes sets the size compaction outputs split at.
func WithTableTargetBytes(size int64) Option {
	return func(o *Options) {
		o.Compaction.TableTargetBytes = size
	}
}

// WithCatalogMaxStaleRecords sets how many dead catalog records trigger a snapshot.
func WithCatalogMaxStaleRecords(n int) Option {
	return func(o *Options) {
		o.Catalog.MaxStaleRecords = n
	}
}
