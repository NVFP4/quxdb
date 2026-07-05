//go:build linux

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

func Fdatasync(file *os.File) error {
	return unix.Fdatasync(int(file.Fd()))
}

func Fallocate(file *os.File, offset int64, n int64) error {
	// Mode 0 ensures the space is allocated and the file size is updated.
	return unix.Fallocate(int(file.Fd()), 0, offset, n)
}
