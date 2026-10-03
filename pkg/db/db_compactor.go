package db

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

const (
	l0FileTarget = 4
	levelFanout  = 10
	// l1 holds a few tables so picking by overlap has a choice
	l1TargetBytes = 256 << 20
)

// compaction outputs split at this size, l0 tables take the memtable's size.
var tableTargetBytes int64 = 64 << 20

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

// dropping a signal is fine, compact runs until levels are within target
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
	// cap l0 jobs so a flush burst isn't one huge compaction
	l0 := version.Level(0)
	if len(l0) >= l0FileTarget {
		inputs := slices.Clone(l0[:l0FileTarget])
		minKey, maxKey := tableRange(inputs)
		inputs = append(inputs, overlapping(version.Level(1), minKey, maxKey)...)
		return 0, inputs
	}

	targets := levelTargets(version)
	for level := 1; level < vset.MaxLevels-1; level++ {
		tables := version.Level(level)
		if tablesSize(tables) <= targets[level] {
			continue
		}

		// move one table at a time to bound write amp and rescore after each edit
		next := version.Level(level + 1)
		source := leastOverlapping(tables, next)
		inputs := []*sst.Metadata{source}
		inputs = append(inputs, overlapping(next, source.MinKey, source.MaxKey)...)
		return level, inputs
	}

	return 0, nil
}

// static targets grow by the fanout from l1, and once the bottom outgrows them each level
// tracks a fanout fraction of the one below, so no level pair drifts past the fanout.
func levelTargets(version *vset.Version) [vset.MaxLevels]int64 {
	var targets [vset.MaxLevels]int64
	static := int64(l1TargetBytes)
	for level := 1; level < vset.MaxLevels-1; level++ {
		targets[level] = static
		static *= levelFanout
	}
	scaled := tablesSize(version.Level(vset.MaxLevels - 1))
	for level := vset.MaxLevels - 2; level >= 1; level-- {
		scaled /= levelFanout
		targets[level] = max(targets[level], scaled)
	}
	return targets
}

// the table that rewrites the fewest next-level bytes per byte it pushes down.
func leastOverlapping(tables, next []*sst.Metadata) *sst.Metadata {
	best, bestRatio := tables[0], math.Inf(1)
	for _, table := range tables {
		ratio := float64(overlapBytes(next, table.MinKey, table.MaxKey)) / float64(max(table.SizeBytes, 1))
		if ratio < bestRatio {
			best, bestRatio = table, ratio
		}
	}
	return best
}

func (c *lsmCompactor) execute(plan *compactionPlan) error {
	defer plan.view.release()

	// moves rewrite nothing, but the last level still merges to drop garbage
	targetLevel := plan.sourceLevel + 1
	if targetLevel < vset.MaxLevels-1 && movable(plan.inputs, targetLevel) {
		if err := c.state.moveTables(plan.inputs, uint8(targetLevel)); err != nil {
			return err
		}
		fmt.Printf("db: moved L%d -> L%d tables=%d\n", plan.sourceLevel, targetLevel, len(plan.inputs))
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

	sources := make([]core.Cursor, 0, len(plan.inputs))
	for _, table := range plan.inputs {
		sources = append(sources, plan.view.tables.Table(table.ID).Cursor(nil, nil))
	}

	targetLevel := plan.sourceLevel + 1
	targetSize := tableTargetBytes
	// tombstones only drop at the last level, where nothing older can resurface
	cur := newMVCCCursor(newMergeCursor(sources), math.MaxUint64, targetLevel < vset.MaxLevels-1)
	opts := sst.BuilderOpts{
		Dir:       c.dataDir,
		Level:     uint8(targetLevel),
		Keys:      estimateOutputKeys(plan.inputs, targetSize),
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
	for {
		key, value, ok := cur.Next()
		if !ok {
			if err := cur.Err(); err != nil {
				return outputs, err
			}
			break
		}

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
			OrderedKey: key,
			FilterKey:  quxKey(key).UserKey(),
			Value:      value,
		}); err != nil {
			return outputs, err
		}
		outputBytes += int64(len(key) + len(value))
	}

	if builder != nil {
		if err := finish(); err != nil {
			return outputs, err
		}
	}
	return outputs, nil
}

// inputs can change level as is when none already sit there and none overlap each other.
func movable(inputs []*sst.Metadata, targetLevel int) bool {
	for _, table := range inputs {
		if int(table.Level) == targetLevel {
			return false
		}
	}
	sorted := slices.SortedFunc(slices.Values(inputs), func(a, b *sst.Metadata) int {
		return bytes.Compare(a.MinKey, b.MinKey)
	})
	for i := 1; i < len(sorted); i++ {
		if bytes.Compare(sorted[i-1].MaxKey, sorted[i].MinKey) >= 0 {
			return false
		}
	}
	return true
}

func overlaps(table *sst.Metadata, minKey, maxKey []byte) bool {
	return bytes.Compare(table.MaxKey, minKey) >= 0 && bytes.Compare(table.MinKey, maxKey) <= 0
}

func overlapping(tables []*sst.Metadata, minKey, maxKey []byte) []*sst.Metadata {
	var result []*sst.Metadata
	for _, table := range tables {
		if overlaps(table, minKey, maxKey) {
			result = append(result, table)
		}
	}
	return result
}

func overlapBytes(tables []*sst.Metadata, minKey, maxKey []byte) int64 {
	var total int64
	for _, table := range tables {
		if overlaps(table, minKey, maxKey) {
			total += int64(table.SizeBytes)
		}
	}
	return total
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

// estimate output keys based on inputs: we don't know the overlap, and bloom needs keys to be known upfront
func estimateOutputKeys(inputs []*sst.Metadata, targetSize int64) uint64 {
	var keys uint64
	for _, table := range inputs {
		keys += table.Keys
	}
	size := uint64(tablesSize(inputs))
	if size == 0 {
		return keys
	}
	return min(keys, uint64(targetSize)*keys/size)
}
