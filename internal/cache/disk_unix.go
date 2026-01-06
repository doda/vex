//go:build linux || darwin || freebsd

package cache

import (
	"syscall"
)

type syscallStatfs = syscall.Statfs_t

func statfs(path string, stat *syscallStatfs) error {
	return syscall.Statfs(path, stat)
}
