//go:build linux || darwin

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

const (
	AdviceRandom     Advice = unix.MADV_RANDOM
	AdviceSequential Advice = unix.MADV_SEQUENTIAL
	AdviceWillNeed   Advice = unix.MADV_WILLNEED
)

// Map maps n bytes of f from off read-only and shared.
func Map(f *os.File, off, n int64) ([]byte, error) {
	b, err := unix.Mmap(int(f.Fd()), off, int(n), unix.PROT_READ, unix.MAP_SHARED)
	return b, pathError("mmap", f, err)
}

// Advise applies advice to a mapping.
func Advise(b []byte, advice Advice) error {
	return unix.Madvise(b, int(advice))
}

// Unmap releases a mapping returned by Map or MapFile.
func Unmap(b []byte) error {
	return unix.Munmap(b)
}
