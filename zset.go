package kaydb

import (
	"errors"
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
)

// ZAdd 将具有指定分数的指定成员添加到存储在key中的有序集合
func (db *KayDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zSetIndex.mu.Lock()
	defer db.zSetIndex.mu.Unlock()

	// 将member写入hash，并进行hash
	if err := db.zSetIndex.murHash.Write(member); err != nil {
		return err
	}
	sum := db.zSetIndex.murHash.EncodeSum128()
	db.zSetIndex.murHash.Reset()

	// key不存在则创建一个
	if db.zSetIndex.trees[string(key)] == nil {
		db.zSetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zSetIndex.trees[string(key)]

	// 将score与key编码成ZSetKey，并写入日志文件
	scoreBuf := []byte(util.Float64ToStr(score))
	zSetKey := db.encodeKey(key, scoreBuf)
	entry := &logfile.LogEntry{Key: zSetKey, Value: member}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}

	// 将member hash后的sum作为key，创建索引树节点
	_, entrySize := logfile.EncodeEntry(entry)
	pos.entrySize = entrySize
	ent := &logfile.LogEntry{Key: sum, Value: member}
	if err = db.updateIndexTree(idxTree, pos, ent, true, ZSet); err != nil {
		return err
	}

	// 往内存ZSet中加入节点
	db.zSetIndex.indexes.ZAdd(string(key), score, string(sum))
	return nil
}

// ZScore 返回key处有序集合中成员的score
func (db *KayDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	// 将member写入hash，获取hash后的sum
	if err := db.zSetIndex.murHash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zSetIndex.murHash.EncodeSum128()
	db.zSetIndex.murHash.Reset()

	// 查找内存ZSet中的score
	return db.zSetIndex.indexes.ZScore(string(key), string(sum))
}

// ZRem 从存储在key中的有序集合中删除指定的成员。忽略不存在的成员
// 当key存在且不包含有序集合时，将返回错误
func (db *KayDB) ZRem(key, member []byte) error {
	db.zSetIndex.mu.Lock()
	defer db.zSetIndex.mu.Unlock()

	// 写入member进行hash
	if err := db.zSetIndex.murHash.Write(member); err != nil {
		return err
	}
	sum := db.zSetIndex.murHash.EncodeSum128()
	db.zSetIndex.murHash.Reset()

	// 调用内存ZSet 的ZRem
	ok := db.zSetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return errors.New("zSetIndex Rem failed")
	}
	// key不存在则创建一个
	if db.zSetIndex.trees[string(key)] == nil {
		db.zSetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zSetIndex.trees[string(key)]

	// 删除树的索引并发送discard
	oldVal, updated := idxTree.Delete(sum)
	db.sendDiscard(oldVal, updated, ZSet)
	// 写入删除类型的日志
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}

	// 删除的entry本身也是无效的
	_, entrySize := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[ZSet].valChan <- node:
	default:
		logger.Warn("send to discard chan failed")
	}
	return nil
}

// ZCard 返回存储在key中的有序集合的基数（元素数），即member的总数
func (db *KayDB) ZCard(key []byte) int {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	return db.zSetIndex.indexes.ZCard(string(key))
}

// ZRange 返回存储在key处的有序集合中指定的元素范围
// 元素被认为是从最低到最高的顺序
func (db *KayDB) ZRange(key []byte, start, end int) ([][]byte, error) {
	return db.zRangeInternal(key, start, end, false)
}

// ZRevRange 返回存储在key处的有序集合中指定的元素范围
// 元素被认为是从最高到最低的顺序
func (db *KayDB) ZRevRange(key []byte, start, end int) ([][]byte, error) {
	return db.zRangeInternal(key, start, end, true)
}

// ZRank 返回存储在key中的有序集合中成员的rank，分数从低到高排序。
// 排名（或索引）从0开始，这意味着得分最低的成员的排名为0
func (db *KayDB) ZRank(key, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, false)
}

// ZRevRank 返回存储在key中的有序集合中成员的rank，分数从高到低排序。
// 排名（或索引）从0开始，这意味着得分最高的成员的排名为0
func (db *KayDB) ZRevRank(key, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, true)
}

// zRange内部实现
func (db *KayDB) zRangeInternal(key []byte, start, end int, rev bool) ([][]byte, error) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	// key不存在则创建一个
	if db.zSetIndex.trees[string(key)] == nil {
		db.zSetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zSetIndex.trees[string(key)]

	var res [][]byte
	var values []interface{}
	// rev为true，则从高到低
	if rev {
		values = db.zSetIndex.indexes.ZRevRange(string(key), start, end)
	} else {
		values = db.zSetIndex.indexes.ZRange(string(key), start, end)
	}

	for _, value := range values {
		v, _ := value.(string)
		// 通过sum获取member
		if val, err := db.getVal(idxTree, []byte(v), ZSet); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}

// zRank内部实现
func (db *KayDB) zRankInternal(key, member []byte, rev bool) (ok bool, rank int) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	// key为空直接返回
	if db.zSetIndex.trees[string(key)] == nil {
		return false, 0
	}
	// 写入member进行hash
	if err := db.zSetIndex.murHash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zSetIndex.murHash.EncodeSum128()
	db.zSetIndex.murHash.Reset()

	var res int64
	// rev为true 分数从高到低排序
	if rev {
		res = db.zSetIndex.indexes.ZRevRank(string(key), string(sum))
	} else {
		res = db.zSetIndex.indexes.ZRank(string(key), string(sum))
	}
	if res != -1 {
		ok = true
		rank = int(res)
	}
	return
}
