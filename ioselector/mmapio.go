package ioselector

import (
	"github.com/studygo/project/rosedb-1.1.0/mmap"
	"io"
	"os"
)

// 内存映射，将内存映射到文件上，修改内存的东西则更新文件

// MMapIOSelector 表示使用内存映射文件I/O
type MMapIOSelector struct {
	fd     *os.File // 文件描述符
	buf    []byte   // mmap的缓冲区
	bufLen int64    // 缓冲区大小
}

// NewMMapIOSelector 创建新的mmap选择器
func NewMMapIOSelector(fName string, fSize int64) (IOSelector, error) {
	if fSize <= 0 {
		return nil, ErrInvalidFSize
	}
	file, err := openFile(fName, fSize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fSize)
	if err != nil {
		return nil, err
	}
	return &MMapIOSelector{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

// 将切片b 拷贝写入偏移处的映射区域（buf）
func (m MMapIOSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	// 偏移量小于0 或 写入数据长度超过 mmap的缓冲区 返回错误EOF
	if offset < 0 || length+offset > m.bufLen {
		return 0, io.EOF
	}
	return copy(m.buf[offset:], b), nil
}

// 将数据从偏移处的映射区域（buf）拷贝读取到切片b中
func (m MMapIOSelector) Read(b []byte, offset int64) (int, error) {
	// 偏移量小于0 或 超过缓冲区
	if offset < 0 || offset >= m.bufLen {
		return 0, io.EOF
	}
	// 读取数据长度超过缓冲区
	if offset+int64(len(b)) >= m.bufLen {
		return 0, io.EOF
	}
	return copy(b, m.buf[offset:]), nil
}

// Sync 将映射的缓冲区与磁盘上的文件内容同步
func (m MMapIOSelector) Sync() error {
	return mmap.Msync(m.buf)
}

// Close 关闭同步/取消映射 映射缓冲区并关闭fd。
func (m MMapIOSelector) Close() error {
	// 将映射的缓冲区与磁盘上的文件内容同步
	if err := mmap.Msync(m.buf); err != nil {
		return err
	}
	// 取消映射先前映射的切片
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	// 关闭fd
	return m.fd.Close()
}

// Delete 删除映射的缓冲区并删除磁盘上的文件
func (m MMapIOSelector) Delete() error {
	// 取消映射先前映射的切片
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	m.buf = nil

	if err := m.fd.Truncate(0); err != nil {
		return err
	}
	// 关闭fd
	if err := m.fd.Close(); err != nil {
		return err
	}
	// 删除fd
	return os.Remove(m.fd.Name())
}
