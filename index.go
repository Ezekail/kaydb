package kaydb

import (
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DataType 定义数据结构类型
type DataType = int8

// 支持5种不同的数据类型, String, List, Hash, Set, Sorted Set
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *KayDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent, pos)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetsIndex(ent, pos)
	case ZSet:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *KayDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	// 如果entry 为删除类型 或到期则删除索引
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}

	// 构建索引节点,先获取entry size
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// 如果是KeyValueMemMode，则把value也存入index
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}
	// 添加索引节点
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *KayDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
	var listKey = ent.Key
	// 如果不是list meta类型，则需要解码获取真正的key
	if ent.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(listKey)
	}
	// key为空则新建一个
	if db.listIndex.trees[string(listKey)] == nil {
		db.listIndex.trees[string(listKey)] = art.NewART()
	}
	idxTree := db.listIndex.trees[string(listKey)]

	// 如果entry为删除类型，则删除索引
	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}
	// 构建索引节点
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// 如果是KeyValueMemMode，则把value也存入index
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}
	// 添加索引节点
	idxTree.Put(ent.Key, idxNode)
}

func (db *KayDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	// 解码hash key
	key, field := db.decodeKey(ent.Key)
	// key 不存在则创建
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	// 如果entry为删除类型，则删除索引
	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(field)
		return
	}

	// 构建索引节点
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// 如果是KeyValueMemMode，则把value也存入index
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}
	// 添加索引节点
	idxTree.Put(field, idxNode)
}

func (db *KayDB) buildSetsIndex(ent *logfile.LogEntry, pos *valuePos) {
	// key 不存在则新建
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(ent.Key)]

	// 如果entry为删除类型，则删除索引
	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Value)
		return
	}

	// 将member写入hash，并进行hash
	if err := db.setIndex.murHash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash : %v", err)
	}
	sum := db.setIndex.murHash.EncodeSum128()
	db.setIndex.murHash.Reset()

	// 构建索引节点
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// 如果是KeyValueMemMode，则把value也存入index
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}
	// 添加索引节点
	idxTree.Put(sum, idxNode)
}

func (db *KayDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	idxTree := db.zSetIndex.trees[string(ent.Key)]
	// 如果entry为删除类型，则删除索引
	if ent.Type == logfile.TypeDelete {
		db.zSetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		if idxTree != nil {
			idxTree.Delete(ent.Value)
		}
		return
	}

	// 解码zSet的key
	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))

	if idxTree == nil {
		idxTree = art.NewART()
	}
	// 将member写入hash，并进行hash
	if err := db.zSetIndex.murHash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.zSetIndex.murHash.EncodeSum128()
	db.zSetIndex.murHash.Reset()

	// 构建索引节点
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}
	db.zSetIndex.indexes.ZAdd(string(key), score, string(sum))
	// 添加索引节点
	idxTree.Put(sum, idxNode)
}

// 从日志文件中加载索引
func (db *KayDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()
		// 从内存中取出fids
		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		// 从小到大排序fids
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		// 遍历fids
		for i, fid := range fids {
			var logFile *logfile.LogFile
			// 取出日志文件，最后一个即为活跃文件
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				logger.Fatalf("log file is nil,failed to open db")
			}
			var offset int64
			for {
				// 读取该日志文件的entry
				entry, eSize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					// 读完日志文件
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				// 赋值内存的valuePos，并构建索引
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += eSize
			}
			// 设置最新日志文件的WriteAt
			if i == len(fids)-1 {
				// 原子性的将val的值保存到*addr
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

func (db *KayDB) updateIndexTree(idxTree *art.AdaptiveRadixTree,
	pos *valuePos, ent *logfile.LogEntry, sendDiscard bool, dType DataType) error {

	// 构建索引节点
	var size = pos.entrySize
	if dType == String || dType == List {
		_, size = logfile.EncodeEntry(ent)
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}

	// 在KeyValueMemMode中，键和值都将存储在内存中
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expireAt = ent.ExpiredAt
	}

	// 修改索引节点
	oldValue, updated := idxTree.Put(ent.Key, idxNode)
	// 若为真，则发送discard文件
	if sendDiscard {
		db.sendDiscard(oldValue, updated, dType)
	}
	return nil
}

func (db *KayDB) getVal(idxTree *art.AdaptiveRadixTree,
	key []byte, dataType DataType) ([]byte, error) {

	// 从内存的自适应基数树中获取索引信息
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}
	// key已过期，返回 ErrKeyNotFound 错误
	now := time.Now().Unix()
	if idxNode.expireAt != 0 && idxNode.expireAt <= now {
		return nil, ErrKeyNotFound
	}

	// 在KeyValueMemMode中，该值存储在内存中
	// 所以从索引信息中获取值
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// 在KeyOnlyMemMode中，值不在内存中，因此从日志文件中获取偏移量
	logFile := db.getActiveLogFile(dataType)
	// 不是活跃文件，则从已归档文件中寻找
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	// 找到的日志文件为空
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}
	// 读取日志文件
	entry, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	// key存在，但无效（已删除或过期）
	if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < now) {
		return nil, ErrKeyNotFound
	}

	return entry.Value, nil
}
