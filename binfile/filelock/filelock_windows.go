// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build windows

package filelock

import (
	"golang.org/x/sys/windows"
	"io/fs"
	"os"
	"strconv"
)

type lockType uint32

const (
	readLock  lockType = 0
	writeLock lockType = windows.LOCKFILE_EXCLUSIVE_LOCK
)

const (
	reserved = 0
	allBytes = ^uint32(0)
)

func lock(f os.File, lt lockType) error {
	// Per https://golang.org/issue/19098, “Programs currently expect the Fd
	// method to return a handle that uses ordinary synchronous I/O.”
	// However, LockFileEx still requires an OVERLAPPED structure,
	// which contains the file offset of the beginning of the lock range.
	// We want to lock the entire file, so we leave the offset as zero.
	ol := new(windows.Overlapped)

	err := windows.LockFileEx(windows.Handle(f.Fd()), uint32(lt), reserved, allBytes, allBytes, ol)
	if err != nil {
		return &fs.PathError{
			Op:   strconv.Itoa(int(lt)),
			Path: f.Name(),
			Err:  err,
		}
	}
	return nil
}

func unlock(f os.File) error {
	ol := new(windows.Overlapped)
	err := windows.UnlockFileEx(windows.Handle(f.Fd()), reserved, allBytes, allBytes, ol)
	if err != nil {
		return &fs.PathError{
			Op:   "Unlock",
			Path: f.Name(),
			Err:  err,
		}
	}
	return nil
}
