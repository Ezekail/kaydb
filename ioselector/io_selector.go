package ioselector

import (
	"errors"
	"os"
)

// ErrInvalidFSize 无效的文件大小
var ErrInvalidFSize = errors.New("fSize can't be zero or negative")

// FilePerm 默认创建的日志文件的权限
const FilePerm = 0644

// IOSelector fileIO 和 mMap的选择器，现在由 wal 和 value log使用
type IOSelector interface {

	// Write 在偏移量处向日志文件写入切片
	// 返回写入的字节数和错误（如果有）
	Write(b []byte, offset int64) (int, error)

	// Read 在偏移量处读取一个切片的数据
	// 它返回读取的字节数和错误（如果有）
	Read(b []byte, offset int64) (int, error)

	// Sync 将文件的当前内容提交到存储器
	// 通常，这意味着将最近写入的数据的文件系统内存副本刷新到磁盘
	Sync() error

	// Close 关闭文件，使其无法用于I/O
	// 如果已关闭，它将返回错误
	Close() error

	// Delete 删除文件
	// 删除前必须关闭它，如果在MMapSelector中，则将取消映射
	Delete() error
}

// 打开文件并在必要时截断
func openFile(fName string, fSize int64) (*os.File, error) {
	file, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	// 返回一个描述name指定的文件对象的FileInfo
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// 如果打开的文件小于fSize，则截断更改文件的大小
	if stat.Size() < fSize {
		if err := file.Truncate(fSize); err != nil {
			return nil, err
		}
	}
	return file, nil
}
