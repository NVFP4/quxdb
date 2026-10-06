package db

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
	"github.com/yashgorana/quxdb/pkg/wal"
)

type lsmState struct {
	versions *vset.VersionSet
	registry *sst.Registry
	current  atomic.Pointer[lsmView]
	mu       sync.Mutex // serializes publishers
}

func newLsmState(dataDir string) (*lsmState, error) {
	versions, err := vset.New(dataDir)
	if err != nil {
		return nil, err
	}

	version := versions.CurrentVersion()
	tables := version.All()
	registry := sst.NewRegistry()
	if err := registry.Open(tables); err != nil {
		return nil, errors.Join(err, registry.Close(), versions.Close())
	}

	// after Open, so every live table is known good before anything is deleted
	if err := sst.RemoveOrphans(dataDir, tables); err != nil {
		fmt.Printf("db: orphan table cleanup error %v\n", err)
	}

	state := &lsmState{versions: versions, registry: registry}
	state.current.Store(&lsmView{
		version:   version,
		tables:    registry.View(tables),
		memtables: []*quxMemtable{newQuxMemtable()},
	})
	publishLevelMetrics(version)
	return state, nil
}

// publishMemtableMetrics sets the memtable gauges from the current view.
func (s *lsmState) publishMemtableMetrics() {
	memtables := s.current.Load().memtables
	var size int
	for _, mt := range memtables {
		size += mt.SizeBytes()
	}
	metrics.DbMemtableBytes.Set(float64(size))
	metrics.DbImmutableMemtables.Set(float64(len(memtables) - 1))
}

func publishLevelMetrics(version *vset.Version) {
	for level := range vset.MaxLevels {
		tables := version.Level(level)
		label := strconv.Itoa(level)
		metrics.SstTables.WithLabelValues(label).Set(float64(len(tables)))
		metrics.SstBytes.WithLabelValues(label).Set(float64(tablesSize(tables)))
	}
}

func (s *lsmState) acquire() *lsmView {
	for {
		view := s.current.Load()
		if view.tables.TryRetain() {
			return view
		}
	}
}

func (s *lsmState) currentVersion() *vset.Version {
	return s.current.Load().version
}

func (s *lsmState) nextTableID() uint64 {
	return s.versions.NextTableID()
}

// write loop only, the sole caller of rolloverMemtable
func (s *lsmState) activeMemtable() *quxMemtable {
	return s.current.Load().memtables[0]
}

// oldest first.
func (s *lsmState) flushableMemtables(retain int) []*quxMemtable {
	immutables := s.current.Load().memtables[1:]
	count := len(immutables) - retain
	if count <= 0 {
		return nil
	}
	flushable := slices.Clone(immutables[len(immutables)-count:])
	slices.Reverse(flushable)
	return flushable
}

func (s *lsmState) immutableMemtableCount() int {
	return len(s.current.Load().memtables) - 1
}

func (s *lsmState) rolloverMemtable(lastSeq quxSeq, lastLSN wal.LSN) *quxMemtable {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.current.Load()
	previous := current.memtables[0]
	previous.lastSeq = lastSeq
	previous.lastLSN = lastLSN

	memtables := make([]*quxMemtable, 0, len(current.memtables)+1)
	memtables = append(memtables, newQuxMemtable())
	memtables = append(memtables, current.memtables...)
	s.current.Store(&lsmView{
		version:   current.version,
		tables:    current.tables,
		memtables: memtables,
	})
	return previous
}

// flushed must be the oldest immutables, oldest first, from a single flusher
func (s *lsmState) replaceMemtablesWithSSTs(flushed []*quxMemtable, toAdd []*sst.Metadata) error {
	last := flushed[len(flushed)-1]
	return s.applyEdit(toAdd, nil, len(flushed), &vset.Checkpoint{
		LastSeq: uint64(last.lastSeq),
		LastLSN: uint64(last.lastLSN),
	})
}

func (s *lsmState) replaceSSTs(toRemove, toAdd []*sst.Metadata) error {
	return s.applyEdit(toAdd, toRemove, 0, nil)
}

// level change only, files and pinned tables stay untouched
func (s *lsmState) moveTables(tables []*sst.Metadata, level uint8) error {
	changes := make([]vset.Change, len(tables))
	for i, table := range tables {
		moved := *table
		moved.Level = level
		changes[i] = vset.Change{Op: vset.OpAdd, Table: &moved}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.versions.Apply(changes); err != nil {
		return err
	}
	current := s.current.Load()
	s.current.Store(&lsmView{
		version:   s.versions.CurrentVersion(),
		tables:    current.tables,
		memtables: current.memtables,
	})
	publishLevelMetrics(s.versions.CurrentVersion())
	return nil
}

// opens toAdd before Apply so a failed open leaves catalog and view unchanged.
func (s *lsmState) applyEdit(
	toAdd []*sst.Metadata,
	toRemove []*sst.Metadata,
	flushed int,
	checkpoint *vset.Checkpoint,
) error {
	changes := make([]vset.Change, 0, len(toAdd)+len(toRemove)+1)
	for _, table := range toAdd {
		changes = append(changes, vset.Change{Op: vset.OpAdd, Table: table})
	}
	for _, table := range toRemove {
		changes = append(changes, vset.Change{Op: vset.OpDelete, Table: table})
	}
	if checkpoint != nil {
		changes = append(changes, vset.Change{Op: vset.OpCheckpoint, Checkpoint: *checkpoint})
	}

	if err := s.registry.Open(toAdd); err != nil {
		s.registry.Retire(toAdd)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.versions.Apply(changes); err != nil {
		s.registry.Retire(toAdd)
		return err
	}
	version := s.versions.CurrentVersion()
	current := s.current.Load()
	s.current.Store(&lsmView{
		version:   version,
		tables:    s.registry.View(version.All()),
		memtables: current.memtables[:len(current.memtables)-flushed],
	})
	current.tables.Release()
	s.registry.Retire(toRemove)
	publishLevelMetrics(version)
	if flushed > 0 {
		s.publishMemtableMetrics()
	}
	return nil
}

func (s *lsmState) close() error {
	s.current.Swap(nil).release()
	return errors.Join(s.registry.Close(), s.versions.Close())
}

type quxMemtable struct {
	memtable.Memtable
	lastLSN wal.LSN
	lastSeq quxSeq
}

func newQuxMemtable() *quxMemtable {
	return &quxMemtable{Memtable: memtable.New(memTableType)}
}
