//go:build windows
// +build windows

package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmap(fd *os.File, write bool, size int64) ([]byte, error) {
	protect := syscall.PAGE_READONLY
	access := syscall.FILE_MAP_READ

	if write {
		protect = syscall.PAGE_READWRITE
		access = syscall.FILE_MAP_WRITE
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// 在windows中，我们不能将文件的大小超过实际大小
	// 因此，将文件截断为mmap的大小
	if fi.Size() < size {
		if err := fd.Truncate(size); err != nil {
			return nil, fmt.Errorf("truncate:%s", err)
		}
	}

	sizelo := uint32(size >> 32)
	sizehi := uint32(size) & 0xffffffff

	// 打开文件映射句柄
	handler, err := syscall.CreateFileMapping(syscall.Handle(fd.Fd()), nil,
		uint32(protect), sizelo, sizehi, nil)
	if err != nil {
		// 返回一个指定系统调用名称和错误细节的SyscallError
		return nil, os.NewSyscallError("CreateFileMapping", err)
	}

	// 创建内存映射
	addr, err := syscall.MapViewOfFile(handler, uint32(access), 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}

	//关闭文件映射句柄
	if err := syscall.CloseHandle(syscall.Handle(handler)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	//切片内存布局
	//从golang/sys包复制了此片段
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(size), int(size)}

	//使用unsafe将sl转换为[]字节。
	data := *(*[]byte)(unsafe.Pointer(&sl))

	return data, nil
}

func munmap(b []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
}

func madvise(b []byte, readahead bool) error {
	// Do Nothing. We don’t care about this setting on Windows
	return nil
}

func msync(b []byte) error {
	return syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}
