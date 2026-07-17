package db

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

const l0FileTarget = 4

// L0 files inherit the memtable's size. Compaction outputs use these target
// data sizes and only exceed them when one user key has too many versions to
// fit without being split across tables.
var tableTargetBytes = [vset.MaxLevels]int64{
	0,
	32 << 20,
	64 << 20,
	128 << 20,
	256 << 20,
}

var levelTargetBytes = [vset.MaxLevels]int64{
	0,            // L0: controlled by l0FileTarget
	320 << 20,    // L1
	3200 << 20,   // L2
	32000 << 20,  // L3
	320000 << 20, // L4
}

type compactionPlan struct {
	sourceLevel  int
	inputs       []vset.Table
	tableVersion *quxTableVersion
}

type lsmCompactor struct {
	dataDir string
	state   *lsmState
	notify  chan struct{}
	workers sync.WaitGroup
}

func newLsmCompactor(dataDir string, state *lsmState) *lsmCompactor {
	return &lsmCompactor{
		dataDir: dataDir,
		state:   state,
		notify:  make(chan struct{}, 1),
	}
}

func (c *lsmCompactor) Start() {
	c.workers.Add(1)
	go c.compactionLoop()
}

// Notify schedules a compaction check without blocking the caller. Multiple
// notifications are coalesced because Compact always runs until the level
// targets are satisfied.
func (c *lsmCompactor) Notify() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *lsmCompactor) Stop() {
	close(c.notify)
	c.workers.Wait()
}

func (c *lsmCompactor) compactionLoop() {
	defer c.workers.Done()
	for range c.notify {
		if err := c.compact(); err != nil {
			fmt.Printf("db: compaction error %v\n", err)
		}
	}
}

func (c *lsmCompactor) compact() error {
	for {
		plan := c.plan()
		if plan == nil {
			return nil
		}
		if err := c.execute(plan); err != nil {
			return err
		}
	}
}

func (c *lsmCompactor) plan() *compactionPlan {
	tableVersion := c.state.acquireTableVersion()
	sourceLevel, inputs := pickCompaction(tableVersion.version)
	if len(inputs) == 0 {
		_ = tableVersion.release()
		return nil
	}
	return &compactionPlan{
		sourceLevel:  sourceLevel,
		inputs:       inputs,
		tableVersion: tableVersion,
	}
}

func pickCompaction(version *vset.Version) (int, []vset.Table) {
	// Prefer L0 once it reaches its file-count trigger. Limit each job to one
	// trigger-sized batch so a flush burst cannot make one compaction map the
	// entire L0 working set at once.
	l0 := version.Level(0)
	if len(l0) >= l0FileTarget {
		inputs := slices.Clone(l0[:l0FileTarget])
		minKey, maxKey := tableRange(inputs)
		inputs = append(inputs, overlapping(version.Level(1), minKey, maxKey)...)
		return 0, inputs
	}

	// The bottom level has no destination, so only L1 through L3 are scored.
	for level := 1; level < vset.MaxLevels-1; level++ {
		tables := version.Level(level)
		size := tablesSize(tables)
		if size <= levelTargetBytes[level] {
			continue
		}

		// Moving one table at a time bounds write amplification and lets the
		// score be recomputed after each version edit. Version tables are in
		// key order, making this choice deterministic.
		source := tables[0]
		inputs := []vset.Table{source}
		inputs = append(inputs, overlapping(
			version.Level(level+1), source.MinKey, source.MaxKey,
		)...)
		return level, inputs
	}

	return 0, nil
}

func (c *lsmCompactor) execute(plan *compactionPlan) error {
	defer func() {
		c.state.reportTableCleanupError(plan.tableVersion.release())
	}()

	toAdd, err := c.buildSSTs(plan)
	if err != nil {
		return err
	}
	if err := c.state.replaceSSTs(plan.inputs, toAdd); err != nil {
		return err
	}

	fmt.Printf("db: compacted L%d -> L%d inputs=%d outputs=%d\n",
		plan.sourceLevel, plan.sourceLevel+1, len(plan.inputs), len(toAdd))
	return nil
}

func (c *lsmCompactor) buildSSTs(plan *compactionPlan) (outputs []*sst.Metadata, err error) {
	var builder *sst.Builder
	defer func() {
		if err == nil {
			return
		}
		if builder != nil {
			err = errors.Join(err, builder.Abort())
		}
		if len(outputs) > 0 {
			err = errors.Join(err, c.state.discardSSTs(outputs))
		}
		outputs = nil
	}()

	readers := make([]sst.Reader, 0, len(plan.inputs))
	cursors := make([]core.Cursor, 0, len(plan.inputs))
	defer func() { closeReaders(readers) }()

	for _, table := range plan.inputs {
		reader, openErr := plan.tableVersion.openTable(sst.Metadata(table))
		if openErr != nil {
			err = openErr
			return
		}
		readers = append(readers, reader)
		cursors = append(cursors, reader.Cursor(nil, nil))
	}

	targetLevel := plan.sourceLevel + 1
	targetSize := tableTargetBytes[targetLevel]
	// The merge may discard many historical versions, so input key counts can
	// grossly over-size the output Bloom filter. Use a bounded density estimate;
	// exceeding it only affects the false-positive rate, not correctness.
	buildOpts := sst.BuilderOpts{
		Dir:       c.dataDir,
		Level:     uint8(targetLevel),
		Keys:      uint64(targetSize/128) + 1,
		SizeBytes: uint64(targetSize),
	}

	merge, mergeErr := newMergeHeap(cursors)
	if mergeErr != nil {
		err = mergeErr
		return
	}
	if len(merge) == 0 {
		err = sst.ErrCorrupt
		return
	}

	outputs = make([]*sst.Metadata, 0, int64(tablesSize(plan.inputs))/targetSize+1)
	startBuilder := func() error {
		next, buildErr := sst.NewBuilder(buildOpts)
		if buildErr != nil {
			return buildErr
		}
		builder = next
		return nil
	}
	finishBuilder := func() error {
		output, buildErr := builder.Finalize()
		if buildErr != nil {
			return buildErr
		}
		builder = nil
		outputs = append(outputs, output)
		return nil
	}

	var outputBytes int64
	var lastResolvedUserKey []byte
	for len(merge) > 0 {
		item := merge[0]
		userKey := item.qkey.UserKey()

		// Internal keys place newest versions first, so the first merged key wins.
		if !bytes.Equal(userKey, lastResolvedUserKey) {
			lastResolvedUserKey = userKey

			emit := item.qkey.Op() != quxOpDelete || targetLevel < vset.MaxLevels-1
			if emit {
				if builder == nil {
					if buildErr := startBuilder(); buildErr != nil {
						err = buildErr
						return
					}
				} else if outputBytes >= targetSize {
					if buildErr := finishBuilder(); buildErr != nil {
						err = buildErr
						return
					}
					outputBytes = 0
					if buildErr := startBuilder(); buildErr != nil {
						err = buildErr
						return
					}
				}

				record := sst.Record{
					OrderedKey: item.qkey,
					FilterKey:  userKey,
					Value:      item.value,
				}
				if buildErr := builder.Add(record); buildErr != nil {
					err = buildErr
					return
				}
				outputBytes += int64(len(record.OrderedKey) + len(record.Value))
			}
		}

		if advanceErr := merge.advanceRoot(); advanceErr != nil {
			err = advanceErr
			return
		}
	}

	if builder != nil {
		if buildErr := finishBuilder(); buildErr != nil {
			err = buildErr
			return
		}
	}
	return
}

func overlapping(tables []vset.Table, minKey, maxKey []byte) []vset.Table {
	var result []vset.Table
	for _, table := range tables {
		if bytes.Compare(table.MaxKey, minKey) >= 0 && bytes.Compare(table.MinKey, maxKey) <= 0 {
			result = append(result, table)
		}
	}
	return result
}

func tableRange(tables []vset.Table) (minKey, maxKey []byte) {
	minKey, maxKey = tables[0].MinKey, tables[0].MaxKey
	for _, table := range tables[1:] {
		if bytes.Compare(table.MinKey, minKey) < 0 {
			minKey = table.MinKey
		}
		if bytes.Compare(table.MaxKey, maxKey) > 0 {
			maxKey = table.MaxKey
		}
	}
	return minKey, maxKey
}

func tablesSize(tables []vset.Table) int64 {
	var total int64
	for _, table := range tables {
		total += int64(table.SizeBytes)
	}
	return total
}

func closeReaders(readers []sst.Reader) {
	for i := range readers {
		_ = readers[i].Close()
	}
}
