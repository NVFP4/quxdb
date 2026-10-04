//go:build linux || darwin

package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const iovMax = 1024

// Advice hints the kernel about the access pattern of a mapping.
type Advice int

var zeroBlock [1 << 20]byte

// SyncDir makes directory entry changes in dir durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// RenameDurable renames oldpath to newpath and syncs the parent dirs so the rename survives a crash.
func RenameDurable(oldpath, newpath string) error {
	if err := os.Rename(oldpath, newpath); err != nil {
		return err
	}
	if oldDir, newDir := filepath.Dir(oldpath), filepath.Dir(newpath); oldDir != newDir {
		if err := SyncDir(oldDir); err != nil {
			return err
		}
	}
	return SyncDir(filepath.Dir(newpath))
}

// ZeroFill writes zeros over [off, off+n), leaving the range backed by written blocks.
func ZeroFill(f *os.File, off, n int64) error {
	for n > 0 {
		chunk := min(n, int64(len(zeroBlock)))
		if _, err := f.WriteAt(zeroBlock[:chunk], off); err != nil {
			return err
		}
		off += chunk
		n -= chunk
	}
	return nil
}

// MapFile maps the whole file at path read-only and applies advice.
func MapFile(path string, advice Advice) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	b, err := Map(f, 0, info.Size())
	if err != nil {
		return nil, err
	}
	if err := Advise(b, advice); err != nil {
		return nil, errors.Join(fmt.Errorf("fs: madvise %s: %w", path, err), Unmap(b))
	}
	return b, nil
}

func pathError(op string, f *os.File, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{Op: op, Path: f.Name(), Err: err}
}

// Pwritev writes bufs to f at off, retrying short writes. It reslices entries of bufs.
func Pwritev(f *os.File, bufs [][]byte, off int64) (int, error) {
	fd := f.Fd()
	n, err := retryPwritev(bufs, off, func(chunk [][]byte, off int64) (int, error) {
		return sysPwritev(fd, chunk, off)
	})
	return n, pathError("pwritev", f, err)
}

func retryPwritev(bufs [][]byte, off int64, sysPwritev func([][]byte, int64) (int, error)) (int, error) {
	total := 0
	for {
		for len(bufs) > 0 && len(bufs[0]) == 0 {
			bufs = bufs[1:]
		}
		if len(bufs) == 0 {
			return total, nil
		}
		n, err := sysPwritev(bufs[:min(len(bufs), iovMax)], off)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
		total += n
		off += int64(n)
		for n > 0 {
			if n < len(bufs[0]) {
				bufs[0] = bufs[0][n:]
				break
			}
			n -= len(bufs[0])
			bufs = bufs[1:]
		}
	}
}
