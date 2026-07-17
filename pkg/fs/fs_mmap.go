//go:build !linux && !darwin

package fs

import "os"

func Mmap(file *os.File, off, length int64) ([]byte, error) {
	panic("mmap not implemented on this platform")
}

func Madvice(b []byte, advice MmapAdvice) error {
	return nil
}

func Munmap(b []byte) error {
	panic("munmap not implemented on this platform")
}
