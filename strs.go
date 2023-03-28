package kaydb

import (
	"bytes"
	"errors"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Set set key保存字符串值，若key已包含一个值，则将覆盖该值
// 成功执行Set操作时，将放弃与key关联的任何先前的生存时间
func (db *KayDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	// 往日志文件写入entry
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// 设置String索引信息，存储在自适应基数树中
	err = db.updateIndexTree(db.strIndex.idxTree, valuePos, entry, true, String)
	return err
}

// Get 获取key的值
// 如果key不存在，则返回错误ErrKeyNotFound
func (db *KayDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

// MGet 获取所有指定key的值
// 如果key不包含字符串值或不存在，则返回nil
func (db *KayDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs

	}

	// 遍历keys，获取key的值
	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// GetDel 获取key的值并删除该key
// 此方法类似Get方法，如果key存在，还会删除该key
func (db *KayDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	db.strIndex.mu.Unlock()

	// 获取key的值
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}

	// 写入删除类型的entry
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return nil, err
	}

	// 删除索引节点
	oldValue, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(oldValue, updated, String)

	// 删除的条目本身也是无效的
	_, entSize := logfile.EncodeEntry(entry)
	node := &indexNode{fid: valPos.fid, entrySize: entSize}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan failed")
	}
	return val, nil
}

// Delete 删除给定key的值
func (db *KayDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 写入删除类型的entry
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	// 删除索引节点
	oldValue, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(oldValue, updated, String)

	// 删除条目本身也是无效的
	_, entSize := logfile.EncodeEntry(entry)
	node := &indexNode{fid: valPos.fid, entrySize: entSize}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard failed")
	}
	return nil
}

// SetEX 设置key以保存字符串，并在给定的持续时间后将key设置为超时
// 如果key存在，则覆盖该值
func (db *KayDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 设置超时时间，将entry写入日志文件
	expiredAt := time.Now().Add(duration).Unix()
	entry := &logfile.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	// 修改索引节点
	return db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
}

// SetNX 如果键值对不存在，则设置该键值对。如果key已经存在，则返回nil
func (db *KayDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 判断key是否存在
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	// key存在
	if val != nil {
		return nil
	}

	// 写入日志文件
	entry := &logfile.LogEntry{Key: key, Value: value}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	// 修改索引节点
	return db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
}

// MSet 设置多对key-value
// 它是多组命令，参数顺序应该像“key”、“value”、“key”和“value”, ...
func (db *KayDB) MSet(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 判断参数是否合法
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	// 添加多个键值对
	for i := 0; i < len(args); i += 2 {
		// 获取参数，写入日志文件
		key, value := args[i], args[i+1]
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}

		// 修改索引节点
		err = db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}

// MSetNX 将给定的key设置为各自的value，若key不存在则不会执行
// 即使只存在一个key，MSetNX也不会执行任何操作
func (db *KayDB) MSetNX(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 校验参数
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrKeyNotFound
	}

	// 首先，检查每个key是否存在
	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}

		// 数据库中存在key。我们丢弃其余的键值对。它提供了方法的原子性
		if val != nil {
			return nil
		}
	}

	// 设置key 的值
	addedKey := make(map[uint64]struct{})
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		// 将key 进行hash
		hash := util.MemHash(key)
		// 若key重复则跳过
		if _, ok := addedKey[hash]; ok {
			continue
		}

		// 写入日志文件
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}

		// 修改索引节点
		err = db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
		if err != nil {
			return err
		}

		// 添加key后记录在map中
		addedKey[hash] = struct{}{}
	}
	return nil
}

// Append 如果key已经存在，Append将在旧值的末尾追加值
// 如果key不存在，则类似Set操作
func (db *KayDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 判断key是否存在
	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	// key已经存在，则将值追加到旧值的末尾
	if oldVal != nil {
		value = append(oldVal, value...)
	}

	// 写入日志文件
	entry := &logfile.LogEntry{Key: key, Value: value}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	// 修改索引节点
	err = db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
	return err
}

// Decr 将键处存储的数字减1
// 如果key不存在，则在执行操作之前将其设置为0
// 如果值不是整数类型，则返回ErrWrongKeyType错误
// 此外，如果值在递减后溢出，则返回ErrIntegerOverflow错误
func (db *KayDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)
}

// DecrBy 将存储在键上的数字递减decr
// 如果key不存在，则在执行操作之前将其设置为0
// 如果值不是整数类型，则返回ErrWrongKeyType错误
// 此外，如果值在递减后溢出，则返回ErrIntegerOverflow错误
func (db *KayDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -decr)
}

// Incr 将键处存储的数字递增1
// 如果key不存在，则在执行操作之前将其设置为0
// 如果值不是整数类型，则返回ErrWrongKeyType错误
// 此外，如果值在增加值后超过，则返回ErrIntegerOverflow错误
func (db *KayDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}

// IncrBy 将键处存储的数字递增
// 如果key不存在，则在执行操作之前将其设置为0
// 如果值不是整数类型，则返回ErrWrongKeyType错误
// 此外，如果值在增加值后超过，则返回ErrIntegerOverflow错误
func (db *KayDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}

// incrDecrBy是Incr、IncrBy、Decr和DecrBy方法的助手方法。它通过incr更新key
func (db *KayDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	// 如果key不存在，则在执行操作之前将其设置为0
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}

	// 如果值不是整数类型，则返回ErrWrongValueType错误
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}

	// 如果值在增加、减少值后溢出，则返回ErrIntegerOverflow错误
	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	// 转为字符，写入日志文件，修改索引节点
	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &logfile.LogEntry{Key: key, Value: val}
	valPos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, valPos, entry, true, String)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// StrLen 返回存储在key中的字符串值的长度。如果key不存在，则返回0
func (db *KayDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	// 判断key是否存在
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(val)
}

// Count 返回String的键总数
func (db *KayDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}

// Scan 遍历String类型的所有key并查找其值
// 参数前缀将匹配key的前缀，模式是一个正则表达式，也匹配key
// 参数count限制key的数量，如果count不是正数，则返回nil切片
// 返回的值将是key和value的混合数据，如[key1，value1，key2，value2等…]
func (db *KayDB) StrScan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	// Regexp是已编译正则表达式的表示形式
	var reg *regexp.Regexp
	if pattern != "" {
		if strings.EqualFold(pattern, "*") {
			pattern = "[\\s\\S]*"
		}
		var err error
		// Compile解析正则表达式，如果成功，则返回可用于与文本匹配的Regexp对象
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}

	// 前缀扫描索引节点，返回count 个 key
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}

	values := make([][]byte, 2*len(keys))
	var index int
	for _, key := range keys {
		// Match报告key是否包含正则表达式reg的任何匹配项
		if reg != nil && !reg.Match(key) {
			continue
		}

		// 获取value
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[index], values[index+1] = key, val
		index += 2
	}
	return values, nil
}

// GetRange 用于获取存储在指定 key 中字符串的子字符串
// 字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)
func (db *KayDB) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return []byte{}, nil
	}
	// 可以使用负偏移量来提供从字符串末尾开始的偏移量。
	// 所以 -1 表示最后一个字符，-2 表示倒数第二个，依此类推
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}
	// 通过将结果范围限制为字符串的实际长度来处理超出范围的请求
	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	return val[start : end+1], nil
}

func (db *KayDB) Scan(cursor int, pattern string, count int) (int, [][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0, nil, nil
	}
	// 迭代string的所有key
	var keys [][]byte
	iter := db.strIndex.idxTree.Iterator()
	now := time.Now().Unix()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return 0, nil, err
		}
		indexNode, _ := node.Value().(*indexNode)
		if indexNode == nil {
			continue
		}
		if indexNode.expireAt != 0 && indexNode.expireAt <= now {
			continue
		}
		keys = append(keys, node.Key())
	}

	// 判断是否匹配
	var reg *regexp.Regexp
	if pattern != "" {
		if strings.EqualFold(pattern, "*") {
			pattern = "[\\s\\S]*"
		}
		var err error
		// Compile解析正则表达式，如果成功，则返回可用于与文本匹配的Regexp对象
		if reg, err = regexp.Compile(pattern); err != nil {
			return 0, nil, err
		}
	}
	var matchingKeys [][]byte
	for k, v := range keys {
		// Match报告key是否包含正则表达式reg的任何匹配项
		if reg != nil && !reg.Match(v) {
			continue
		}
		if k > cursor {
			matchingKeys = append(matchingKeys, v)
		}
	}

	// 限制扫描结果的数量
	var scanResults [][]byte
	for i := 0; i < count && i < len(matchingKeys); i++ {
		scanResults = append(scanResults, matchingKeys[i])
	}

	// 设置新的游标
	newCursor := 0
	if len(matchingKeys) > count && len(scanResults) > 0 {
		newCursor = len(scanResults) - 1
	}

	return newCursor, scanResults, nil
}
