package bufpool

import (
	"math/bits"
	"sync"
)

const (
	nBuckets      = 13
	minBucketSize = 1024                            // min 1 KiB
	maxBucketSize = minBucketSize << (nBuckets - 1) // max 4 MiB
)

var (
	bufferPools   [nBuckets]sync.Pool
	log2MinBucket = bits.Len(uint(minBucketSize - 1))
)

func init() {
	for i := range nBuckets {
		size := minBucketSize << i
		bufferPools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
}

func Get(size uint) []byte {
	pIdx := poolIndex(size)
	if pIdx < 0 {
		return make([]byte, size) // too large m8, heap it
	}

	bufPtr := bufferPools[pIdx].Get().(*[]byte)
	return (*bufPtr)[:size]
}

func Put(b []byte) {
	c := cap(b)
	if c > maxBucketSize {
		// was heap allocated, let gc manage it
		return
	}

	idx := poolIndex(uint(c))
	bufferPools[idx].Put(&b)
}

func poolIndex(size uint) int {
	if size <= 1024 {
		return 0
	}
	// cooler way to do ceil(log2)
	idx := bits.Len(size-1) - log2MinBucket
	if idx >= nBuckets {
		return -1
	}
	return idx
}
