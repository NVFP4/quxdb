//go:build darwin

package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

func Fdatasync(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

func Fallocate(file *os.File, offset int64, n int64) error {
	fd := file.Fd()

	// try a strict contiguous allocation
	fstore := unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  offset,
		Length:  n,
	}

	if err := unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore); err == nil {
		return nil
	}

	// on error, retry without F_ALLOCATECONTIG
	fstore.Flags = unix.F_ALLOCATEALL
	return unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore)
}
