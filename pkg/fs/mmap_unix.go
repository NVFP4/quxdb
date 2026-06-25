//go:build linux || darwin

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

func Mmap(file *os.File, off, length int64) ([]byte, error) {
	return unix.Mmap(int(file.Fd()), off, int(length), unix.PROT_READ, unix.MAP_SHARED)
}

func Munmap(b []byte) error {
	return unix.Munmap(b)
}
