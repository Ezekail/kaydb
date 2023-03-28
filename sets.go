package kaydb

import (
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
)

// SAdd 将指定的members添加到存储在key处的set
// 已是此集合member的指定members将被忽略
// 如果key不存在，则在添加指定members之前创建一个新的set
func (db *KayDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	// key不存在，创建一个新的set
	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(key)]
	for _, member := range members {
		if len(member) == 0 {
			continue
		}
		// 将member写入hash，并进行hash
		if err := db.setIndex.murHash.Write(member); err != nil {
			return err
		}
		sum := db.setIndex.murHash.EncodeSum128()
		db.setIndex.murHash.Reset()

		// entry写入到日志
		entry := &logfile.LogEntry{Key: key, Value: member}
		pos, err := db.writeLogEntry(entry, Set)
		if err != nil {
			return err
		}

		//  key为哈希编码后的值，写入到索引
		ent := &logfile.LogEntry{Key: sum, Value: member}
		_, entrySize := logfile.EncodeEntry(ent)
		pos.entrySize = entrySize
		if err = db.updateIndexTree(idxTree, pos, ent, true, Set); err != nil {
			return err
		}
	}
	return nil
}

// SPop 从key处的set值存储中删除并返回一个或多个随机members
func (db *KayDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()
	// key 为空，返回nil
	if db.setIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	// 返回一个迭代器,用于对叶节点进行预排序遍历
	iterator := idxTree.Iterator()
	// HasNext 如果遍历树时迭代有更多节点，则返回true
	for iterator.HasNext() && count > 0 {
		count--
		// 返回树中的下一个元素并前进迭代器位置。如果树中没有更多节点，则返回ErrNoMoreNodes错误
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		// 获取member
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}

	// 删除节点
	for _, value := range values {
		if err := db.sRemInternal(key, value); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem 从存储在key处的set中删除指定的members
// 将忽略不是此set member的指定members
// 如果key不存在，则将其视为空集，此命令返回0
func (db *KayDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	// 如果key不存在，则将其视为空集
	if db.setIndex.trees[string(key)] == nil {
		return nil
	}
	for _, member := range members {
		if err := db.sRemInternal(key, member); err != nil {
			return err
		}
	}
	return nil
}

// SIsMember 如果member是存储在key中的set的member，则返回
func (db *KayDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// key不存在，返回false
	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setIndex.trees[string(key)]
	// 将member写入hash，并进行hash
	if err := db.setIndex.murHash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murHash.EncodeSum128()
	db.setIndex.murHash.Reset()

	// 通过sum获取节点
	node := idxTree.Get(sum)
	return node != nil
}

// SMembers 返回存储在key中的集合值的所有member
func (db *KayDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.sMembers(key)
}

// SCard 返回存储在key中的set的集合基数（元素数），即树中的member总数
func (db *KayDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.setIndex.trees[string(key)]
	return idxTree.Size()
}

// SDiff 返回第一个set和所有连续set之间的差集的成员
// 如果没有key作为参数传递，则返回错误
// 求的是在第一个集合中但不在其他集合中的元素集
func (db *KayDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// 没有key，返回错误
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	// 一个key，则返回set的members
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	// 获取第一个key的members
	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}
	successiveSet := make(map[uint64]struct{})
	// 遍历剩下的key
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			// 将member hash后放入successiveSet中
			memHash := util.MemHash(member)
			if _, ok := successiveSet[memHash]; !ok {
				successiveSet[memHash] = struct{}{}
			}
		}
	}
	// 如果剩下的key为空，则返回firstSet
	if len(successiveSet) == 0 {
		return firstSet, nil
	}
	res := make([][]byte, 0)
	// 遍历firstSet，将member hash后，判断是否存在于successiveSet，不存在则追加入结果
	for _, member := range firstSet {
		memHash := util.MemHash(member)
		if _, ok := successiveSet[memHash]; !ok {
			res = append(res, member)
		}
	}
	return res, nil
}

// SUnion 返回由所有给定set的并集产生的集合的成员
func (db *KayDB) SUnion(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// key为空，返回错误
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	// 只有一个key，直接返回该key的members
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	set := make(map[uint64]struct{})
	unionSet := make([][]byte, 0)
	for _, key := range keys {
		// 获取key 中的所有members
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			// 将member hash 后放入set中，若不存在，则追加入unionSet中
			memHash := util.MemHash(member)
			if _, ok := set[memHash]; !ok {
				set[memHash] = struct{}{}
				unionSet = append(unionSet, member)
			}
		}
	}
	return unionSet, nil
}

// 删除set中指定的member
func (db *KayDB) sRemInternal(key, member []byte) error {
	idxTree := db.setIndex.trees[string(key)]
	// 将member写入hash，并进行hash
	if err := db.setIndex.murHash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murHash.EncodeSum128()
	db.setIndex.murHash.Reset()

	// 删除索引节点
	oldVal, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}
	// 写入删除类型的entry，value为sum
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}
	// 发送discard文件
	db.sendDiscard(oldVal, updated, Set)
	// 删除的entry本身也是无效的
	_, entrySize := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[Set].valChan <- node:
	default:
		logger.Warn("send to discard chan failed")
	}
	return nil
}

// sMembers 是获取给定set key的所有member的帮助方法
func (db *KayDB) sMembers(key []byte) ([][]byte, error) {
	// key不存在
	if db.setIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	var values [][]byte
	idxTree := db.setIndex.trees[string(key)]
	// 返回一个迭代器,用于对叶节点进行预排序遍历
	iterator := idxTree.Iterator()
	// HasNext 如果遍历树时迭代有更多节点，则返回true
	for iterator.HasNext() {
		// 返回树中的下一个元素并前进迭代器位置。如果树中没有更多节点，则返回ErrNoMoreNodes错误
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		// 获取节点
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}
