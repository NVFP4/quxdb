//go:build !linux && !darwin

package fs

import "os"

func Mmap(file *os.File, off, length int64) ([]byte, error) {
	panic("mmap not implemented on this platform")
}

func Munmap(b []byte) error {
	panic("munmap not implemented on this platform")
}
