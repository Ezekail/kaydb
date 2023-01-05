package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// MaxHeaderSize 最大entry header 大小
// crc32	typ    kSize	vSize	expiredAt
//
//	4    +   1   +   5   +   5    +    10      = 25（参考binary.MaxVarintLen32和binary.MMaxVarintLen64）
const MaxHeaderSize = 25

// EntryType entry的类型
type EntryType byte

const (
	// TypeDelete 删除类型的entry
	TypeDelete EntryType = iota + 1
	// TypeListMeta 列表元数据类型的entry
	TypeListMeta
)

// LogEntry 追加到日志文件的数据
type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64 // time.Unix
	Type      EntryType
}

type entryHeader struct {
	crc32     uint32 // crc校验和
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // 过期时间 time.Unix
}

// EncodeEntry 将entry编码为字节切片,返回切片与切片大小
// 编码后的 Entry :
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//
//	|--------------------------crc check---------------------------|
func EncodeEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
	// 编码 entry 头部
	header[4] = byte(e.Type)
	var index = 5
	// 将一个int64数字编码写入buf并返回写入的长度，如果buf太小，则会panic
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	// 开辟了entry的size，赋值头部
	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:])

	// 存放key 和 value
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	// crc32. 返回数据data使用IEEE多项式计算出的CRC-32校验和
	crc := crc32.ChecksumIEEE(buf[4:])
	// 编码crc
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

// 解码 entry Header 返回header 与 size
func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		// 解码crc校验值
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5
	// 从buf解码一个int64，返回该数字和读取的字节长度
	// kSize
	kSize, n := binary.Varint(buf[index:])
	h.kSize = uint32(kSize)
	index += n
	// vSize
	vSize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vSize)
	index += n
	// 过期时间
	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt

	return h, int64(index + n)
}

// 获取校验和
func getEntryCrc(e *LogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(h[:])
	// 返回将切片p的数据采用tab表示的多项式添加到crc之后计算出的新校验和
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
