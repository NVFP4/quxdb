package quxdb

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/yashgorana/quxdb/pkg/wal"
)

func TestWALFailureMakesDBReadOnly(t *testing.T) {
	dir := t.TempDir()
	db, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	t.Cleanup(func() { _ = db.Stop(t.Context()) })

	require.NoError(t, db.Set([]byte("before"), []byte("v")))
	failActiveSegmentWrites(t, dir)

	err = db.Set([]byte("after"), []byte("v"))
	require.ErrorIs(t, err, ErrDbReadOnly)
	require.ErrorIs(t, err, wal.ErrWALFailed)
	require.ErrorIs(t, db.Err(), ErrDbReadOnly)

	// later writes fail without reaching the wal, reads keep working
	require.ErrorIs(t, db.Set([]byte("later"), []byte("v")), ErrDbReadOnly)
	require.ErrorIs(t, db.Delete([]byte("before")), ErrDbReadOnly)
	got, found, err := db.Get([]byte("before"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v"), got)
	_, found, err = db.Get([]byte("after"))
	require.NoError(t, err)
	assert.False(t, found)
}

// failActiveSegmentWrites swaps /dev/full into the active segment's descriptor so every write fails with ENOSPC.
func failActiveSegmentWrites(t *testing.T, dir string) {
	t.Helper()
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	require.NoError(t, err)
	defer full.Close()

	walDir, err := filepath.Abs(filepath.Join(dir, "wal"))
	require.NoError(t, err)
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	for _, entry := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil || filepath.Dir(target) != walDir || !strings.HasSuffix(target, ".quxwal") {
			continue
		}
		fd, err := strconv.Atoi(entry.Name())
		require.NoError(t, err)
		require.NoError(t, unix.Dup3(int(full.Fd()), fd, unix.O_CLOEXEC))
		return
	}
	t.Fatal("no open wal segment found")
}
