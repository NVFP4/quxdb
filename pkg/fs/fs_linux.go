package fs

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// SyncData makes f's data and the metadata needed to read it durable.
func SyncData(f *os.File) error {
	return pathError("fdatasync", f, unix.Fdatasync(int(f.Fd())))
}

// Reserve allocates blocks for the first size bytes of f without changing its size.
func Reserve(f *os.File, size int64) error {
	return pathError("fallocate", f, unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size))
}

// Preallocate allocates blocks for the first size bytes of f and grows it to size, new bytes read as zero.
func Preallocate(f *os.File, size int64) error {
	return pathError("fallocate", f, unix.Fallocate(int(f.Fd()), 0, 0, size))
}

// raw syscall with a stack iovec array, unix.Pwritev allocates one per call
func sysPwritev(fd uintptr, bufs [][]byte, off int64) (int, error) {
	var iovs [iovMax]unix.Iovec
	n := 0
	for _, b := range bufs {
		if len(b) == 0 {
			continue
		}
		iovs[n] = unix.Iovec{Base: &b[0]}
		iovs[n].SetLen(len(b))
		n++
	}
	if n == 0 {
		return 0, nil
	}
	hi := uintptr(uint64(off) >> (unix.SizeofLong*8 - 1) >> 1)
	written, _, errno := unix.Syscall6(unix.SYS_PWRITEV, fd, uintptr(unsafe.Pointer(&iovs[0])), uintptr(n), uintptr(off), hi, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(written), nil
}
