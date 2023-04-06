package kaydb

import (
	"encoding/binary"
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
)

// LPush 将所有指定值插入存储在key的list的开头
// 如果键不存在，则在执行推送操作之前将其创建为空列表
func (db *KayDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, true); err != nil {
			return err
		}
	}
	return nil
}

// LPushX 在存储在key中的list的开头插入指定的值，仅当key已经存在并持有列表时
// 与LPush相反，当key不存在时，不会执行任何操作
func (db *KayDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush 将所有指定值插入存储在key中的列表尾部
// 如果键不存在，则在执行推送操作之前将其创建为空列表
func (db *KayDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, false); err != nil {
			return err
		}
	}
	return nil
}

// RPushX 在存储在key的list尾部插入指定的值，仅当key已经存在并持有列表时
// 与RPush相反，当key不存在时，将不执行任何操作
func (db *KayDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop 删除并返回存储在key中的列表的第一个元素
func (db *KayDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	return db.popInternal(key, true)
}

// RPop 删除并返回存储在key中的列表的最后一个元素
func (db *KayDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LMove 原子地返回并删除存储在源位置的列表的第一个/最后一个元素
// 并将该元素推送到存储在目标位置的列表中的第一个或最后一个
func (db *KayDB) LMove(srcKey, dstKey []byte, srcIsLeft, dstIsLeft bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// Pop第一个或最后一个元素
	popValue, err := db.popInternal(srcKey, srcIsLeft)
	if err != nil {
		return nil, err
	}
	if popValue == nil {
		return nil, ErrKeyNotFound
	}

	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewART()
	}

	// 将返回的值插入到dstKey的第一个或最后一个元素
	if err = db.pushInternal(dstKey, popValue, dstIsLeft); err != nil {
		return nil, err
	}
	return popValue, nil
}

// LLen 返回存储在key中的列表的长度
// 若key不存在，则将其解释为空列表，并返回0
func (db *KayDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.listIndex.trees[string(key)]
	// 通过seq计算key中列表长度
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return 0
	}
	return int(tailSeq - headSeq - 1)
}

// LIndex 返回存储在key的列表中index的元素
// 如果索引超出范围，则返回nil
func (db *KayDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	// key不存在返回nil
	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	// 转换index
	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}
	// 索引越界
	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrWrongIndex
	}
	// 将seq编码到key中，返回对应的value
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// LSet 将索引处的列表元素设置为元素,修改index处的元素
func (db *KayDB) LSet(key, value []byte, index int) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	// 查找key的列表元数据
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}

	// 转换index
	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return err
	}
	// 索引越界
	if seq >= tailSeq || seq <= headSeq {
		return ErrWrongIndex
	}

	// 将seq编码到key，修改对应的value，写入日志
	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: value}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	// 修改索引
	if err = db.updateIndexTree(idxTree, pos, ent, true, List); err != nil {
		return err
	}
	return nil
}

// LRange 返回存储在key中的列表的指定元素
// 偏移量start和end是从零开始的索引，0是列表的第一个元素（列表的开头），1是下一个元素，依此类推
// 这些偏移也可以是负数，表示从列表末尾开始的偏移，例如，-1是列表的最后一个元素，-2是倒数第二个元素，依此类推
// 如果start大于列表的结尾，则返回一个空列表
// 如果end大于列表的实际结尾，Redis会将其视为列表的最后一个元素
func (db *KayDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// key 不存在返回nil
	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	// 获取列表数据类型元数据信息
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	var startSeq, endSeq uint32

	// 转换start和end
	startSeq, err = db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}
	endSeq, err = db.listSequence(headSeq, tailSeq, end)
	if err != nil {
		return nil, err
	}

	// 标准化startSeq 和 endSeq
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}
	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}
	// 索引越界
	if startSeq >= tailSeq || endSeq <= headSeq || startSeq > endSeq {
		return nil, ErrWrongIndex
	}

	// 包括endSeq的值
	for seq := startSeq; seq < endSeq+1; seq++ {
		// 遍历，编码seq到key，获取对应的val
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil

}

// LScan 返回存储在list中的所有key
func (db *KayDB) LScan() []string {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	// 获取list 的所有key
	index := db.listIndex.trees
	keys := make([]string, 0)
	for key := range index {
		keys = append(keys, key)
	}
	return keys
}

// 编码seq，往key头部插入seq
func (db *KayDB) encodeListKey(key []byte, seq uint32) []byte {
	// 编码seq，存入buf
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

// 解码seq，返回key和seq
func (db *KayDB) decodeListKey(buf []byte) ([]byte, uint32) {
	// 解码seq
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

// 列表元数据
func (db *KayDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	// 获取value
	val, err := db.getVal(idxTree, key, List)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	// 初始化列表
	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		// 解码
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

// 保存列表的元数据，写入日志文件
func (db *KayDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	// 编码 seq 写入entry
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(idxTree, pos, ent, true, List)
	return err

}

// listSequence 只是将逻辑index转换为物理seq
// 无论物理seq是否合法，只要转换它
func (db *KayDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32
	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq + uint32(-index)
	}
	return seq, nil
}

// LPush or RPush value
func (db *KayDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	idxTree := db.listIndex.trees[string(key)]

	// 获取列表元数据
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}

	// LPush 或 RPush
	var seq = headSeq
	if !isLeft {
		seq = tailSeq
	}

	// 编码key，写入entry, entryKey: key + seq Value : value
	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: val}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}

	if err = db.updateIndexTree(idxTree, pos, ent, true, List); err != nil {
		return err
	}
	// 更新seq
	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}

	// 保存列表元数据
	err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
	return err
}

// LPop or RPop Value
func (db *KayDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	// key不存在，则直接返回
	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	idxTree := db.listIndex.trees[string(key)]
	// 获取列表元数据
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	// 特殊判断，防止key中没有更新seq的情况
	size := tailSeq - headSeq - 1
	if size <= 0 {
		// 重置元数据
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		}
		return nil, nil
	}
	// 获取该key的seq
	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}

	// 编码key，获取到存储的value
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	// 写入删除类型的entry
	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	// 删除索引节点
	oldVal, updated := idxTree.Delete(encKey)
	// 更新seq
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	// 更新列表元数据
	if err = db.saveListMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	// send discard
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- node:
	default:
		logger.Warn("send to discard chan failed")
	}
	return val, nil
}
