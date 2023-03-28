//go:build windows
// +build windows

package flock

import "syscall"

// FileLockGuard 保存目录上的文件锁
type FileLockGuard struct {
	// 目录上的文件描述符
	fd syscall.Handle
}

// AcquireFileLock 通过syscall.Flock获取目录上的锁
// 返回FileLockGuard或错误（如果有）
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	// UTF16PtrFromString 返回指向UTF-8字符串s的UTF-16编码的指针，并添加了终止NUL。
	// 如果s在任何位置包含NUL字节，则返回（nil，EINVAL)
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	var access, mode uint32
	if readOnly {
		// 一般读取
		access = syscall.GENERIC_READ
		// 文件共享读  |  文件共享写
		mode = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE
	} else {
		// 一般读取  |  一般写入
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}

	// 创建文件
	file, err := syscall.CreateFile(ptr, access, mode, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	// 若文件不存在则创建
	if err == syscall.ERROR_FILE_NOT_FOUND {
		file, err = syscall.CreateFile(ptr, access, mode, nil,
			syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

// SyncDir do nothing in windows.
func SyncDir(name string) error {
	return nil
}

// Release 释放文件锁
func (fl *FileLockGuard) Release() error {
	return syscall.Close(fl.fd)
}
