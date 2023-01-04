package ioselector

import "os"

// FileIOSelector 表示使用标准文件IO
type FileIOSelector struct {
	fd *os.File // 系统文件描述符
}

// NewFileIOSelector 创建新的文件io选择器
func NewFileIOSelector(fName string, fSize int64) (IOSelector, error) {
	if fSize <= 0 {
		return nil, ErrInvalidFSize
	}
	file, err := openFile(fName, fSize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

// 封装os.File WriteAt
func (f FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return f.fd.WriteAt(b, offset)
}

// 封装os.File ReadAt
func (f FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

// Sync 封装os.File Sync
func (f FileIOSelector) Sync() error {
	return f.fd.Sync()
}

// Close 封装os.File Close
func (f FileIOSelector) Close() error {
	return f.fd.Close()
}

// Delete 如果不再使用文件描述符，请删除它
// 封装os.Remove
func (f FileIOSelector) Delete() error {
	if err := f.fd.Close(); err != nil {
		return err
	}
	return os.Remove(f.fd.Name())
}
