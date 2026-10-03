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

var tableTargetBytes = [vset.MaxLevels]int64{
	0, // l0 tables take the memtable's size
	32 << 20,
	64 << 20,
	128 << 20,
	256 << 20,
}

var levelTargetBytes = [vset.MaxLevels]int64{
	0, // l0 is triggered by file count, see l0FileTarget
	320 << 20,
	3200 << 20,
	32000 << 20,
	320000 << 20,
}

type compactionPlan struct {
	sourceLevel int
	inputs      []*sst.Metadata
	view        *lsmView
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

// dropping a signal is fine since compact runs until every level is within target
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
	view := c.state.acquire()
	sourceLevel, inputs := pickCompaction(view.version)
	if len(inputs) == 0 {
		view.release()
		return nil
	}
	return &compactionPlan{
		sourceLevel: sourceLevel,
		inputs:      inputs,
		view:        view,
	}
}

func pickCompaction(version *vset.Version) (int, []*sst.Metadata) {
	// cap each l0 job at l0FileTarget files so a flush burst doesn't turn into one huge compaction
	l0 := version.Level(0)
	if len(l0) >= l0FileTarget {
		inputs := slices.Clone(l0[:l0FileTarget])
		minKey, maxKey := tableRange(inputs)
		inputs = append(inputs, overlapping(version.Level(1), minKey, maxKey)...)
		return 0, inputs
	}

	for level := 1; level < vset.MaxLevels-1; level++ {
		tables := version.Level(level)
		size := tablesSize(tables)
		if size <= levelTargetBytes[level] {
			continue
		}

		// move one table at a time to bound write amp and rescore after each edit
		source := tables[0]
		inputs := []*sst.Metadata{source}
		inputs = append(inputs, overlapping(
			version.Level(level+1), source.MinKey, source.MaxKey,
		)...)
		return level, inputs
	}

	return 0, nil
}

func (c *lsmCompactor) execute(plan *compactionPlan) error {
	defer plan.view.release()

	// a lone table changes level in the catalog only; merges into the last level still rewrite
	// so shadowed versions and tombstones get dropped there.
	targetLevel := plan.sourceLevel + 1
	if len(plan.inputs) == 1 && targetLevel < vset.MaxLevels-1 {
		table := plan.inputs[0]
		if err := c.state.moveTable(table, uint8(targetLevel)); err != nil {
			return err
		}
		fmt.Printf("db: moved L%d -> L%d table=%d\n", plan.sourceLevel, targetLevel, table.ID)
		return nil
	}

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
		c.state.registry.Retire(outputs)
		outputs = nil
	}()

	cursors := make([]core.Cursor, 0, len(plan.inputs))
	for _, table := range plan.inputs {
		cursors = append(cursors, plan.view.tables.Table(table.ID).Cursor(nil, nil))
	}
	merge, err := newMergeHeap(cursors)
	if err != nil {
		return nil, err
	}
	if len(merge) == 0 {
		return nil, sst.ErrCorrupt
	}

	targetLevel := plan.sourceLevel + 1
	targetSize := tableTargetBytes[targetLevel]
	bottomLevel := targetLevel == vset.MaxLevels-1
	// size the bloom filter from the target, not input key counts, which include versions the merge drops
	opts := sst.BuilderOpts{
		Dir:       c.dataDir,
		Level:     uint8(targetLevel),
		Keys:      uint64(targetSize/128) + 1,
		SizeBytes: uint64(targetSize),
	}
	finish := func() error {
		output, err := builder.Finalize()
		if err != nil {
			return err
		}
		builder = nil
		outputs = append(outputs, output)
		return nil
	}

	var outputBytes int64
	var lastUserKey []byte
	for len(merge) > 0 {
		item := merge[0]
		userKey := item.qkey.UserKey()

		// newest version sorts first, so older versions of the same key are dropped
		if !bytes.Equal(userKey, lastUserKey) {
			lastUserKey = userKey

			if !bottomLevel || item.qkey.Op() != quxOpDelete {
				if builder != nil && outputBytes >= targetSize {
					if err := finish(); err != nil {
						return outputs, err
					}
				}
				if builder == nil {
					opts.ID = c.state.nextTableID()
					if builder, err = sst.NewBuilder(opts); err != nil {
						return outputs, err
					}
					outputBytes = 0
				}
				if err := builder.Add(sst.Record{
					OrderedKey: item.qkey,
					FilterKey:  userKey,
					Value:      item.value,
				}); err != nil {
					return outputs, err
				}
				outputBytes += int64(len(item.qkey) + len(item.value))
			}
		}

		if err := merge.advanceRoot(); err != nil {
			return outputs, err
		}
	}

	if builder != nil {
		if err := finish(); err != nil {
			return outputs, err
		}
	}
	return outputs, nil
}

func overlapping(tables []*sst.Metadata, minKey, maxKey []byte) []*sst.Metadata {
	var result []*sst.Metadata
	for _, table := range tables {
		if bytes.Compare(table.MaxKey, minKey) >= 0 && bytes.Compare(table.MinKey, maxKey) <= 0 {
			result = append(result, table)
		}
	}
	return result
}

func tableRange(tables []*sst.Metadata) (minKey, maxKey []byte) {
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

func tablesSize(tables []*sst.Metadata) int64 {
	var total int64
	for _, table := range tables {
		total += int64(table.SizeBytes)
	}
	return total
}
