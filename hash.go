package kaydb

import (
	"bytes"
	"errors"
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
	"math"
	"regexp"
	"strconv"
)

// HSet 将存储在key处的哈希中的field设置为value。如果key不存在，则创建一个保存哈希的新key
// 如果哈希中已存在field，则将覆盖该字段
// 返回指定key的哈希中的元素数
// 接受多字段值对,参数顺序应该像“key”,“field”,“value”,“filed”,“value”...
func (db *KayDB) HSet(key []byte, args ...[]byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	// 奇数个参数
	if len(args) == 0 || len(args)&1 == 1 {
		return ErrWrongNumberOfArgs
	}
	// key不存在，则创建一个
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	// 添加多个字段值对
	for i := 0; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		// 编码hash key，并写入日志
		hashKey := db.encodeKey(key, field)
		ent := &logfile.LogEntry{Key: hashKey, Value: value}
		pos, err := db.writeLogEntry(ent, Hash)
		if err != nil {
			return err
		}

		// 构建索引 索引的key为field
		entry := &logfile.LogEntry{Key: field, Value: value}
		_, entrySize := logfile.EncodeEntry(ent)
		pos.entrySize = entrySize
		err = db.updateIndexTree(idxTree, pos, entry, true, Hash)
		if err != nil {
			return err
		}
	}
	return nil
}

// HSetNX 仅在field不存在时设置给定值,如果key不存在，则创建新的哈希
// 如果filed已经存在，HSetNX不会产生副作用
func (db *KayDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	// key不存在，则创建一个
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}

	// 获取索引节点
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)

	// filed存在，则直接返回false
	if val != nil {
		return false, nil
	}

	// 编码key，写入日志
	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: value}
	pos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return false, err
	}

	// 索引的key为filed
	entry := &logfile.LogEntry{Key: field, Value: value}
	_, entrySize := logfile.EncodeEntry(ent)
	pos.entrySize = entrySize
	if err = db.updateIndexTree(idxTree, pos, entry, true, Hash); err != nil {
		return false, err
	}
	return true, nil
}

// HGet 返回与存储在key中的哈希中的filed关联的值
func (db *KayDB) HGet(key, filed []byte) ([]byte, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	// key 不存在，直接返回nil
	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}

	// 通过key查询filed的value
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, filed, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// HMGet 返回与key处存储的哈希中的指定多个filed相关联的值
// 对于hash中不存在的每个字段，将返回一个nil值
// 因为不存在的keys被视为空hashes，所以对不存在的key运行HMGet将返回一个nil值列表
func (db *KayDB) HMGet(key []byte, fields ...[]byte) (vals [][]byte, err error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// key不存在，返回一个对应field数量的nil列表
	length := len(fields)
	if db.hashIndex.trees[string(key)] == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals, nil
	}

	// key 存在
	idxTree := db.hashIndex.trees[string(key)]
	for _, field := range fields {
		val, err := db.getVal(idxTree, field, Hash)
		if err == ErrKeyNotFound {
			vals = append(vals, nil)
		} else {
			vals = append(vals, val)
		}
	}
	return
}

// HDel 从存储在key的哈希中删除指定的字段
// 忽略此哈希中不存在的指定字段
// 若key不存在，则将其视为空哈希，此命令返回0
func (db *KayDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	// key不存在，返回0
	if db.hashIndex.trees[string(key)] == nil {
		return 0, ErrKeyNotFound
	}
	idxTree := db.hashIndex.trees[string(key)]

	// 遍历fields，删除field
	var count int
	for _, field := range fields {
		// 编码key，写入删除类型的entry到日志
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Type: logfile.TypeDelete}
		pos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}

		// 索引删除节点，count++
		oldVal, updated := idxTree.Delete(field)
		if updated {
			count++
		}
		db.sendDiscard(oldVal, updated, Hash)

		// 删除的entry本身也是无效的
		_, entrySize := logfile.EncodeEntry(entry)
		node := &indexNode{fid: pos.fid, entrySize: entrySize}
		select {
		case db.discards[Hash].valChan <- node:
		default:
			logger.Warn("send to discard failed")

		}
	}
	return count, nil
}

// HExists 返回存储在key中的哈希中是否存在该字段
// 如果哈希包含字段，则返回true
// 如果哈希不包含field或key不存在，则返回false
func (db *KayDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// key不存在返回false
	if db.hashIndex.trees[string(key)] == nil {
		return false, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}
	return val != nil, nil
}

// HLen 返回存储在key中的哈希中包含的字段数
func (db *KayDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// key不存在返回0
	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.hashIndex.trees[string(key)]
	return idxTree.Size()
}

// HKeys 返回存储在key中的哈希中的所有字段名
func (db *KayDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// keys 不存在则直接返回空的列表
	var keys [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return keys, ErrKeyNotFound
	}

	// 返回一个迭代器,用于对叶节点进行预排序遍历
	iterator := tree.Iterator()
	// HasNext 如果遍历树时迭代有更多节点，则返回true
	for iterator.HasNext() {
		// 返回树中的下一个元素并前进迭代器位置。如果树中没有更多节点，则返回ErrNoMoreNodes错误
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

// HVals 返回存储在key中的哈希中的所有值
func (db *KayDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// key为空，返回空的列表
	var values [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return values, ErrKeyNotFound
	}

	// 返回一个迭代器,用于对叶节点进行预排序遍历
	iterator := tree.Iterator()
	// HasNext 如果遍历树时迭代有更多节点，则返回true
	for iterator.HasNext() {
		// 返回树中的下一个元素并前进迭代器位置。如果树中没有更多节点，则返回ErrNoMoreNodes错误
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(tree, node.Key(), Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// HGetAll 返回存储在key中的哈希的所有字段和值
func (db *KayDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	// key不存在，返回空的列表
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return [][]byte{}, nil
	}

	var index int
	pairs := make([][]byte, tree.Size()*2)
	iterator := tree.Iterator()
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		field := node.Key()
		val, err := db.getVal(tree, field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		pairs[index], pairs[index+1] = field, val
		index += 2
	}
	return pairs[:index], nil
}

// HScan 遍历Hash类型的指定键并查找其字段和值。
// 参数前缀将匹配字段的前缀，pattern是一个正则表达式，也匹配字段。
// 参数count限制key的数量，如果count不是正数，则返回nil切片。
// 返回的值将是字段和值的混合数据，如[field1、value1、field2、value2等…]
func (db *KayDB) HScan(key, prefix []byte, pattern string, count int) ([][]byte, error) {
	// 参数count限制filed的数量，如果count不是正数，则返回nil切片。
	if count <= 0 {
		return nil, nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	// key为空，则返回nil
	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}

	// 前缀扫描
	idxTree := db.hashIndex.trees[string(key)]
	fields := idxTree.PrefixScan(prefix, count)
	if len(fields) == 0 {
		return nil, nil
	}

	// Regexp是已编译正则表达式的表示形式
	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		// Compile解析正则表达式，如果成功，则返回可用于与文本匹配的Regexp对象
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	values := make([][]byte, len(fields)*2)
	var index int
	for _, field := range fields {
		// Match报告key是否包含正则表达式reg的任何匹配项,不符合正则表达式的
		if reg != nil && !reg.Match(field) {
			continue
		}
		// 获取val
		val, err := db.getVal(idxTree, field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values[index], values[index+1] = field, val
		index += 2
	}
	return values, nil
}

// HIncrBy 将存储在key中的哈希中的字段中的数字递增
// 如果key不存在，将创建一个新的key，该key包含哈希值
// 如果字段不存在，则在执行操作之前将值设置为0,HIncrBy支持的值范围限于64位有符号整数
func (db *KayDB) HIncrBy(key, field []byte, incr int64) (int64, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	// 如果key不存在，将创建一个新的key，该key包含哈希值
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}

	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	// 若val为空则置为0
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	// 将val转为int64
	valInt64, err := util.StrToInt64(string(val))
	if err != nil {
		return 0, ErrWrongValueType
	}
	// val int64溢出
	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	// 递增val
	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	// 将修改后的val写入日志
	hashKey := db.encodeKey(key, field)
	entry := &logfile.LogEntry{Key: hashKey, Value: val}
	pos, err := db.writeLogEntry(entry, Hash)
	if err != nil {
		return 0, err
	}
	// 更新索引
	ent := &logfile.LogEntry{Key: field, Value: val}
	_, entrySize := logfile.EncodeEntry(ent)
	pos.entrySize = entrySize
	if err = db.updateIndexTree(idxTree, pos, ent, true, Hash); err != nil {
		return 0, err
	}
	return valInt64, nil
}
