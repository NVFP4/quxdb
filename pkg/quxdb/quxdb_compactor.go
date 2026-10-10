package quxdb

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

type compactionPlan struct {
	sourceLevel int
	inputs      []*sst.Metadata
	view        *lsmView
}

type lsmCompactor struct {
	dataDir     string
	state       *lsmState
	opts        CompactionOptions
	sstOpts     sst.Options
	log         *slog.Logger
	notify      chan struct{}
	workers     sync.WaitGroup
	onCompacted func() // runs after each compaction pass
}

func newLsmCompactor(dataDir string, state *lsmState, opts Options, onCompacted func()) *lsmCompactor {
	return &lsmCompactor{
		dataDir:     dataDir,
		state:       state,
		opts:        opts.Compaction,
		sstOpts:     opts.SST,
		log:         opts.Logger.With("mod", "compactor"),
		notify:      make(chan struct{}, 1),
		onCompacted: onCompacted,
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
			metrics.DbCompactionErrors.Inc()
			c.log.Error("compaction failed", "err", err)
		}
		c.onCompacted()
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
	sourceLevel, inputs := pickCompaction(view.version, c.opts)
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		c.logPlan(view.version, sourceLevel, inputs)
	}
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

// logPlan reports the picked inputs, or that none were picked, against each level's size and target.
func (c *lsmCompactor) logPlan(version *vset.Version, sourceLevel int, inputs []*sst.Metadata) {
	var sizes [vset.MaxLevels]int64
	for level := range vset.MaxLevels {
		sizes[level] = tablesSize(version.Level(level))
	}
	targets := levelTargets(version, c.opts)
	l0 := len(version.Level(0))
	if len(inputs) == 0 {
		c.log.Debug("nothing to compact", "l0Tables", l0, "l0FileTarget", c.opts.L0FileTarget,
			"levelBytes", sizes, "levelTargets", targets)
		return
	}
	ids := make([]uint64, len(inputs))
	for i, table := range inputs {
		ids[i] = table.ID
	}
	minKey, maxKey := tableRange(inputs)
	c.log.Debug("compaction picked", "from", sourceLevel, "inputs", ids, "minKey", shortKey(minKey),
		"maxKey", shortKey(maxKey), "l0Tables", l0, "levelBytes", sizes, "levelTargets", targets)
}

// shortKey keeps at most 32 bytes of a user key for logs, handlers quote and escape it.
func shortKey(key []byte) string {
	if len(key) > 32 {
		return string(key[:32]) + "…"
	}
	return string(key)
}

func pickCompaction(version *vset.Version, opts CompactionOptions) (int, []*sst.Metadata) {
	// cap l0 jobs so a flush burst isn't one huge compaction
	l0 := version.Level(0)
	if len(l0) >= opts.L0FileTarget {
		inputs := slices.Clone(l0[:opts.L0FileTarget])
		minKey, maxKey := tableRange(inputs)
		inputs = append(inputs, overlapping(version.Level(1), minKey, maxKey)...)
		return 0, inputs
	}

	targets := levelTargets(version, opts)
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
func levelTargets(version *vset.Version, opts CompactionOptions) [vset.MaxLevels]int64 {
	var targets [vset.MaxLevels]int64
	static := opts.L1TargetBytes
	for level := 1; level < vset.MaxLevels-1; level++ {
		targets[level] = static
		static *= int64(opts.LevelFanout)
	}
	scaled := tablesSize(version.Level(vset.MaxLevels - 1))
	for level := vset.MaxLevels - 2; level >= 1; level-- {
		scaled /= int64(opts.LevelFanout)
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
	start := time.Now()

	// moves rewrite nothing, but the last level still merges to drop garbage
	targetLevel := plan.sourceLevel + 1
	if targetLevel < vset.MaxLevels-1 && movable(plan.inputs, targetLevel) {
		if err := c.state.moveTables(plan.inputs, uint8(targetLevel)); err != nil {
			return err
		}
		took := time.Since(start)
		metrics.DbCompactionMove.Observe(took.Seconds())
		c.log.Info("moved tables", "from", plan.sourceLevel, "to", targetLevel, "tables", len(plan.inputs), "took", took)
		return nil
	}

	toAdd, err := c.buildSSTs(plan)
	if err != nil {
		return err
	}
	if err := c.state.replaceSSTs(plan.inputs, toAdd); err != nil {
		return err
	}
	took := time.Since(start)
	read, written := tablesSize(plan.inputs), tablesSize(toAdd)
	metrics.DbCompactionMerge.Observe(took.Seconds())
	metrics.DbCompactionBytesRead.Add(float64(read))
	metrics.DbCompactionBytesWritten.Add(float64(written))

	c.log.Info("compacted", "from", plan.sourceLevel, "to", targetLevel, "inputs", len(plan.inputs), "outputs", len(toAdd),
		"bytesRead", read, "bytesWritten", written, "took", took)
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

	sources := make([]core.Iterator, 0, len(plan.inputs))
	for _, table := range plan.inputs {
		sources = append(sources, plan.view.tables.Table(table.ID).Iterator(nil, nil))
	}

	targetLevel := plan.sourceLevel + 1
	targetSize := c.opts.TableTargetBytes
	drop := canDropTombstones(plan.view.version, plan.sourceLevel, plan.inputs)
	c.log.Debug("merging", "from", plan.sourceLevel, "to", targetLevel, "dropTombstones", drop)
	cur := newSnapshotIterator(newMergeIterator(sources), math.MaxUint64, drop)
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
		c.log.Debug("compaction output", "id", output.ID, "level", output.Level, "keys", output.Keys, "bytes", output.SizeBytes)
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
			if builder, err = sst.NewBuilderWithOptions(opts, c.sstOpts); err != nil {
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

// true when no other table at l1 or deeper overlaps the inputs
func canDropTombstones(version *vset.Version, sourceLevel int, inputs []*sst.Metadata) bool {
	minKey, maxKey := tableRange(inputs)
	for level := max(sourceLevel, 1); level < vset.MaxLevels; level++ {
		for _, table := range version.Level(level) {
			if overlaps(table, minKey, maxKey) && !slices.ContainsFunc(inputs, func(in *sst.Metadata) bool {
				return in.ID == table.ID
			}) {
				return false
			}
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
