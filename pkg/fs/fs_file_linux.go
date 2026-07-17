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
	// reserve blocks, without increasing the file size
	return unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_KEEP_SIZE, offset, n)
}
