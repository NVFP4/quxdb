package bufpool

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetReturnsRequestedLength(t *testing.T) {
	sizes := []uint{
		0,
		1,
		minBucketSize - 1,
		minBucketSize,
		minBucketSize + 1,
		maxBucketSize - 1,
		maxBucketSize,
		maxBucketSize + 1, // bypasses the pool
		4 * maxBucketSize,
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			buf := Get(size)
			require.Len(t, buf.B, int(size))
			assert.Equal(t, int(size), cap(buf.B))

			for i := range buf.B {
				buf.B[i] = byte(i)
			}
			buf.Release()
		})
	}
}

func TestRoundTripKeepsBucketInvariant(t *testing.T) {
	for i := range nBuckets {
		size := uint(minBucketSize << i)

		first := Get(size)
		require.Len(t, first.B, int(size))
		first.Release()

		second := Get(size)
		require.Len(t, second.B, int(size))
		assert.Equal(t, int(size), cap(second.B))
		second.Release()
	}
}

func TestReassignedBufferCannotReachThePool(t *testing.T) {
	buf := Get(minBucketSize)
	orig := &buf.B[0]

	buf.B = append(buf.B, 0) //nolint:gocritic // deliberately re-homes B
	require.NotSame(t, orig, &buf.B[0], "append must reallocate")
	require.Greater(t, cap(buf.B), int(minBucketSize))

	buf.Release()

	for range 4 {
		got := Get(minBucketSize)
		require.Len(t, got.B, int(minBucketSize))
		got.Release()
	}
}

func TestReleaseTwiceIsNoOp(t *testing.T) {
	const size = uint(minBucketSize)

	buf := Get(size)
	buf.Release()
	buf.Release()

	first, second := Get(size), Get(size)
	require.NotSame(t, &first.B[0], &second.B[0], "pool handed out one buffer twice")

	first.Release()
	second.Release()
}

func TestReleaseZeroesBuf(t *testing.T) {
	buf := Get(minBucketSize)
	buf.Release()

	assert.Nil(t, buf.B)
	assert.Nil(t, buf.ptr)
}

func TestOversizedRequestBypassesPool(t *testing.T) {
	buf := Get(maxBucketSize + 1)
	require.Len(t, buf.B, int(maxBucketSize+1))
	assert.Nil(t, buf.ptr, "oversized request must not hold a pool pointer")

	buf.Release()
	assert.Nil(t, buf.B)
}

func TestGetReleaseDoesNotAllocate(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	for i := range nBuckets {
		size := uint(minBucketSize << i)

		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			for range 8 {
				buf := Get(size)
				buf.Release()
			}

			got := testing.AllocsPerRun(200, func() {
				buf := Get(size)
				buf.B[0] = 1
				buf.Release()
			})
			assert.Zero(t, got, "warm round trip allocated")
		})
	}
}

func TestOversizedRequestAllocatesOnce(t *testing.T) {
	got := testing.AllocsPerRun(50, func() {
		buf := Get(maxBucketSize + 1)
		buf.B[0] = 1
		buf.Release()
	})
	assert.Equal(t, float64(1), got)
}
