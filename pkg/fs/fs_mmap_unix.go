//go:build linux || darwin

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

const (
	MADV_RANDOM     MmapAdvice = unix.MADV_RANDOM
	MADV_SEQUENTIAL MmapAdvice = unix.MADV_SEQUENTIAL
	MADV_WILLNEED   MmapAdvice = unix.MADV_WILLNEED
)

func Mmap(file *os.File, off, length int64) ([]byte, error) {
	return unix.Mmap(int(file.Fd()), off, int(length), unix.PROT_READ, unix.MAP_SHARED)
}

func Madvice(b []byte, advice MmapAdvice) error {
	return unix.Madvise(b, int(advice))
}

func Munmap(b []byte) error {
	return unix.Munmap(b)
}
