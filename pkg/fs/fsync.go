//go:build !linux && !darwin

package fs

import "os"

func Fdatasync(file *os.File) error {
	panic("fdatasync not implemented on this platform")
}

func Fallocate(file *os.File, offset int64, n int64) error {
	panic("fallocate not implemented on this platform")
}
