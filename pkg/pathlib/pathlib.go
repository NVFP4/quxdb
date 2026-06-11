package pathlib

import (
	"os"
	"path/filepath"
)

func IsWritable(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Mode().Perm()&0o200 != 0
}

func EnsureDir(path string) error {
	// already exists
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	return os.MkdirAll(path, 0o755)
}

func EnsureParent(path string) error {
	dir := filepath.Dir(path)
	return EnsureDir(dir)
}

func DirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func FileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

func FileEmpty(path string) bool {
	if !FileExists(path) {
		return true
	}
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Size() == 0
}
