package mmap

import "os"

// Mmap 使用Mmap系统调用内存映射文件。如果可写为true，则会设置页面的内存保护，以便它们也可以被写入
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// Munmap 取消映射先前映射的切片
func Munmap(b []byte) error {
	return munmap(b)
}

// Madvise  当使用内存映射到文件的切片时，Madvise使用Madvise系统调用提供关于内存使用的建议
// 如果页面引用是随机顺序的，请将readahead标志设置为false
func Madvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

// Msync 将对映射的数据调用sync
func Msync(b []byte) error {
	return msync(b)
}
