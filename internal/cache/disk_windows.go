//go:build windows
// +build windows

package cache

import (
	"syscall"
	"unsafe"
)

type syscallStatfs struct {
	Bsize  uint64
	Blocks uint64
}

var (
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceExW  = kernel32.NewProc("GetDiskFreeSpaceExW")
)

func statfs(path string, stat *syscallStatfs) error {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}

	var freeBytesAvailable, totalBytes, freeBytes uint64
	r1, _, err := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&freeBytes)),
	)
	if r1 == 0 {
		return err
	}

	stat.Bsize = 1
	stat.Blocks = totalBytes
	return nil
}
