package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

// SyncData makes f's data durable on stable storage, F_FULLFSYNC also flushes the drive cache.
func SyncData(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return pathError("fcntl F_FULLFSYNC", f, err)
}

// Reserve allocates blocks for the first size bytes of f without changing its size.
func Reserve(f *os.File, size int64) error {
	fd := f.Fd()
	var st unix.Stat_t
	if err := unix.Fstat(int(fd), &st); err != nil {
		return pathError("fstat", f, err)
	}
	// F_PREALLOCATE extends from the physical end of file, not from an offset
	missing := size - st.Blocks*512
	if missing <= 0 {
		return nil
	}

	fstore := unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Length:  missing,
	}
	if err := unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore); err == nil {
		return nil
	}
	fstore.Flags = unix.F_ALLOCATEALL
	return pathError("fcntl F_PREALLOCATE", f, unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore))
}

// Preallocate allocates blocks for the first size bytes of f and grows it to size, new bytes read as zero.
func Preallocate(f *os.File, size int64) error {
	if err := Reserve(f, size); err != nil {
		return err
	}
	return f.Truncate(size)
}

func sysPwritev(fd uintptr, bufs [][]byte, off int64) (int, error) {
	return unix.Pwritev(int(fd), bufs, off)
}
