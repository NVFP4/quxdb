//go:build linux || darwin

package fs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreallocateGrowsFileWithZeros(t *testing.T) {
	f := createFile(t, []byte("head"))

	require.NoError(t, Preallocate(f, 3<<20))
	assert.Equal(t, int64(3<<20), fileSize(t, f))

	got := make([]byte, 3<<20)
	_, err := f.ReadAt(got, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("head"), got[:4])
	assert.Equal(t, make([]byte, len(got)-4), got[4:])
}

func TestReserveKeepsFileSize(t *testing.T) {
	f := createFile(t, []byte("head"))

	require.NoError(t, Reserve(f, 1<<20))
	assert.Equal(t, int64(4), fileSize(t, f))
}

func TestZeroFillClearsOnlyTheRange(t *testing.T) {
	data := bytes.Repeat([]byte{0xab}, 3<<20)
	f := createFile(t, data)

	require.NoError(t, ZeroFill(f, 10, 2<<20))

	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	want := bytes.Clone(data)
	clear(want[10 : 10+2<<20])
	assert.Equal(t, want, got)
}

func TestRenameDurableMovesFile(t *testing.T) {
	dir := t.TempDir()
	oldpath := filepath.Join(dir, "old")
	newpath := filepath.Join(dir, "sub", "new")
	require.NoError(t, os.WriteFile(oldpath, []byte("data"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Dir(newpath), 0o755))

	require.NoError(t, RenameDurable(oldpath, newpath))

	_, err := os.Stat(oldpath)
	require.ErrorIs(t, err, os.ErrNotExist)
	got, err := os.ReadFile(newpath)
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got)
}

func TestMapFileMapsWholeFile(t *testing.T) {
	f := createFile(t, []byte("mapped bytes"))

	b, err := MapFile(f.Name(), AdviceRandom)
	require.NoError(t, err)
	assert.Equal(t, []byte("mapped bytes"), b)
	require.NoError(t, Unmap(b))
}

func TestErrorsNameTheFile(t *testing.T) {
	path := createFile(t, []byte("data")).Name()
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	err = Preallocate(f, 1<<20)
	var pathErr *os.PathError
	require.True(t, errors.As(err, &pathErr), "got %v", err)
	assert.Equal(t, path, pathErr.Path)
}

func createFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "file"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	_, err = f.Write(data)
	require.NoError(t, err)
	return f
}

func fileSize(t *testing.T, f *os.File) int64 {
	t.Helper()
	info, err := f.Stat()
	require.NoError(t, err)
	return info.Size()
}

func TestPwritevRetriesShortWritesAndEINTR(t *testing.T) {
	bufs, want := testBufs(1500)

	var out []byte
	calls := 0
	n, err := retryPwritev(bufs, 100, func(chunk [][]byte, off int64) (int, error) {
		calls++
		require.LessOrEqual(t, len(chunk), iovMax)
		if calls == 1 {
			return 0, syscall.EINTR
		}
		require.Equal(t, int64(100+len(out)), off)
		// write at most 7 bytes per call, splitting buffers mid-way
		written := 0
		for _, b := range chunk {
			take := min(len(b), 7-written)
			out = append(out, b[:take]...)
			written += take
			if written == 7 {
				break
			}
		}
		return written, nil
	})
	require.NoError(t, err)
	assert.Equal(t, len(want), n)
	assert.Equal(t, want, out)
}

func TestPwritevFailsWithoutProgress(t *testing.T) {
	bufs, _ := testBufs(3)
	n, err := retryPwritev(bufs, 0, func([][]byte, int64) (int, error) {
		return 0, nil
	})
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.Zero(t, n)
}

func TestPwritevWritesFile(t *testing.T) {
	f := createFile(t, nil)

	bufs, want := testBufs(2500)
	n, err := Pwritev(f, bufs, 4096)
	require.NoError(t, err)
	assert.Equal(t, len(want), n)

	got := make([]byte, len(want))
	_, err = f.ReadAt(got, 4096)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// testBufs returns n buffers of varying length, some empty, and their concatenation.
func testBufs(n int) ([][]byte, []byte) {
	bufs := make([][]byte, n)
	var want []byte
	for i := range bufs {
		bufs[i] = bytes.Repeat([]byte{byte(i)}, i%5)
		want = append(want, bufs[i]...)
	}
	return bufs, want
}
