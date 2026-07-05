//go:build darwin

package fs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func Fdatasync(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

func Fallocate(file *os.File, offset int64, n int64) error {
	fd := file.Fd()

	fstore := unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  offset,
		Length:  n,
	}

	err := unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore)
	if err != nil {
		return err
	}

	if err := unix.Ftruncate(int(fd), n); err != nil {
		return fmt.Errorf("ftruncate logic pointer update failed: %w", err)
	}

	return nil
}
