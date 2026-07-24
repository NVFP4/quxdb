package bufpool

import (
	"math/bits"
	"sync"
)

const (
	minShift      = 10
	minBucketSize = 1 << minShift // min 1 KiB

	maxShift      = 22
	maxBucketSize = 1 << maxShift // max 4 MiB

	nBuckets = maxShift - minShift + 1
)

var bufferPools [nBuckets]sync.Pool

func init() {
	for i := range nBuckets {
		size := minBucketSize << i
		bufferPools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
}

type Buf struct {
	B []byte

	ptr *[]byte
	idx int8
}

// Get borrows byte slice of non-zeroed content from a pool
func Get(size uint) Buf {
	pIdx := poolIndex(size)
	if pIdx < 0 {
		return Buf{B: make([]byte, size)} // too large m8, heap it
	}

	bufPtr := bufferPools[pIdx].Get().(*[]byte)
	return Buf{B: (*bufPtr)[:size:size], ptr: bufPtr, idx: int8(pIdx)}
}

// Release returns the buffer back to the pool
func (b *Buf) Release() {
	if b.ptr != nil {
		bufferPools[b.idx].Put(b.ptr)
	}

	*b = Buf{}
}

func poolIndex(size uint) int {
	idx := max(0, bits.Len(size-1)-minShift)
	if idx >= nBuckets {
		return -1
	}
	return idx
}
