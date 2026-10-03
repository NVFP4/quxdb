package db

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestEstimateOutputKeys(t *testing.T) {
	inputs := []*sst.Metadata{
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
	}

	// a large target can't hold more keys than the inputs have
	assert.Equal(t, uint64(400), estimateOutputKeys(inputs, 1<<20))
	// a small target holds what fits at the inputs' 10 bytes per key
	assert.Equal(t, uint64(100), estimateOutputKeys(inputs, 1000))
	// inputs without data fall back to their key count
	assert.Equal(t, uint64(2), estimateOutputKeys([]*sst.Metadata{{Keys: 2}}, 1000))
}
