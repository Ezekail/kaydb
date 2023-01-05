package logfile

import (
	"errors"
	"fmt"
	"github.com/Ezekail/kaydb/ioselector"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCrc 无效的crc校验值
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	// ErrWriteSizeNotEqual 写入大小不等于entry 大小
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	// ErrEndOfEntry 日志文件中的entry 结尾,读到最后一条之后
	ErrEndOfEntry = errors.New("logfile: end of entry in log file ")

	// ErrUnsupportedLogFileType 不支持的日志文件类型，现在只支持WAL和ValueLog
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")

	// ErrUnsupportedIOType 不支持的io类型，现在只支持mmap和fileIO
	ErrUnsupportedIOType = errors.New("unsupported IO type")
)

const (
	// InitialLogFileId 初始文件Id
	InitialLogFileId = 0

	// FilePrefix 日志文件前缀
	FilePrefix = "log."
)

// FileType 表示不同类型的日志文件：wal和value-log
type FileType uint8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	// FileNamesMap type -> name
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zsets.",
	}

	// FileTypesMap name -> type
	FileTypesMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

// IOType 表示不同类型的文件io：FileIO（标准文件io）和MMap（内存映射）
type IOType int8

const (
	// FileIO （标准文件io）
	FileIO IOType = iota
	// MMap （内存映射）
	MMap
)

// LogFile 磁盘文件的抽象，entry的读取和写入将经过它
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IOSelector ioselector.IOSelector
}

// OpenLogFile 打开现有日志文件或创建新日志文件
// fsize必须是正数。我们将根据ioType创建io选择器
func OpenLogFile(path string, fid uint32, fSize int64, fType FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	// 创建日志文件名
	fileName, err := lf.getLogFileName(path, fid, fType)
	if err != nil {
		return nil, err
	}
	// 创建io选择器判断类型
	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fSize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapIOSelector(fileName, fSize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIOType
	}
	lf.IOSelector = selector
	return
}

// 获取日志文件名字
func (lf *LogFile) getLogFileName(path string, fid uint32, fType FileType) (name string, err error) {
	// 不存在的文件类型
	if _, ok := FileNamesMap[fType]; !ok {
		return "", ErrUnsupportedLogFileType
	}
	// 拼接日志文件名
	fName := FileNamesMap[fType] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fName)
	return
}

// 从偏移量处读取一个切片的数据
func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IOSelector.Read(buf, offset)
	return
}

// ReadLogEntry 从日志文件的偏移量处读取LogEntry，它返回LogEntry、entry 大小和错误（如果有）
// 如果偏移量无效，则错误为io.EOF
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// 读取 entry header
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}

	// 解码 entry header
	header, size := decodeHeader(headerBuf)
	// 读取到最后一条entries
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// 读取 entry key and value
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 check.
	// 将header 中除crc外，生成crc校验值，判断是否跟之前相等
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

// 以偏移量读取日志文件中的字节切片，切片长度为给定大小
// 它返回字节切片和错误（如果有）
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IOSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// 在日志文件末尾写入一个字节片
// 返回错误（如果有）
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) < 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IOSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

// Sync 将日志文件的当前内容提交到稳定存储
func (lf *LogFile) Sync() error {
	return lf.IOSelector.Sync()
}

// Close 关闭当前日志文件
func (lf *LogFile) Close() error {
	return lf.IOSelector.Close()
}

// Delete 删除当前日志文件
// 如果这样做，文件将无法检索，因此请小心使用
func (lf *LogFile) Delete() error {
	return lf.IOSelector.Delete()
}
