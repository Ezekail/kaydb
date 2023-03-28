package util

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"io"
)

// 非加密哈希函数

type Murmur128 struct {
	mur murmur3.Hash128
}

// NewMurmur128 初始化哈希函数
func NewMurmur128() *Murmur128 {
	return &Murmur128{
		mur: murmur3.New128(),
	}
}

// 将数据写入hash
func (m *Murmur128) Write(p []byte) error {
	n, err := m.mur.Write(p)
	if n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

// EncodeSum128 哈希编码
func (m *Murmur128) EncodeSum128() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	s1, s2 := m.mur.Sum128()
	var index int
	index += binary.PutUvarint(buf[index:], s1)
	index += binary.PutUvarint(buf[index:], s2)
	return buf[:index]
}

// Reset 将哈希重置为其初始状态
func (m *Murmur128) Reset() {
	m.mur.Reset()
}
