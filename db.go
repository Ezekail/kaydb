package kaydb

import (
	"encoding/binary"
	"errors"
	"github.com/Ezekail/kaydb/datastruct/art"
	"github.com/Ezekail/kaydb/datastruct/zset"
	"github.com/Ezekail/kaydb/flock"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"github.com/Ezekail/kaydb/util"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrKeyNotFound 找不到key
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound 找不到日志文件
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrWrongNumberOfArgs 键值对数量不匹配
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	// ErrIntegerOverflow int64溢出限制
	ErrIntegerOverflow = errors.New("increment or decrement overflow")

	// ErrWrongValueType value不是数字
	ErrWrongValueType = errors.New("value is not an integer")

	// ErrWrongIndex  超出索引范围
	ErrWrongIndex = errors.New("index is out of range")
)

const (
	logFileTypeNum   = 5                  // 日志文件类型数量
	encodeHeaderSize = 10                 // 编码头部大小
	initialListSeq   = math.MaxUint32 / 2 // 初始列表Seq
	discardFilePath  = "DISCARD"          // 丢弃文件路径
	lockFileName     = "FLOCK"            // 锁文件名字
)

type (
	// KayDB 数据库实例
	KayDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile // 活跃日志文件
		archivedLogFiles map[DataType]archivedFiles    // 归档日志文件

		fidMap   map[DataType][]uint32 // 文件id的map，仅在启动时使用，即使日志文件已更改也从不更新，文件id加载到内存
		discards map[DataType]*discard // 丢弃文件的map
		opts     Options

		strIndex  *strIndex  // String indexes(adaptive-radix-tree). 索引（自适应基树）
		listIndex *listIndex // List indexes.
		hashIndex *hashIndex // Hash indexes.
		setIndex  *setIndex  // Set indexes.
		zSetIndex *zSetIndex // Sorted set indexes.

		mu       sync.RWMutex
		fileLock *flock.FileLockGuard // 目录上的文件锁
		closed   uint32               // 是否关闭 1：已关闭
		gcState  int32                // gc 状态 1：执行中  0：未执行
	}
	archivedFiles map[uint32]*logfile.LogFile // 归档文件map

	// value 节点
	valuePos struct {
		fid       uint32 // 文件id
		offset    int64  // 偏移量
		entrySize int    // entry 大小
	}
	// 索引节点
	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expireAt  int64 // 过期时间
	}

	strIndex struct { // str 索引
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}
	listIndex struct { // list 索引
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}
	hashIndex struct { // hash 索引
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}
	setIndex struct { // set 索引
		mu      *sync.RWMutex
		murHash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
	zSetIndex struct { // zSet 索引
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murHash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

// 初始化Strs 索引
func newStrsIndex() *strIndex {
	return &strIndex{mu: new(sync.RWMutex), idxTree: art.NewART()}
}

// 初始化List 索引
func newListIndex() *listIndex {
	return &listIndex{mu: new(sync.RWMutex), trees: make(map[string]*art.AdaptiveRadixTree)}
}

// 初始化Hash 索引
func newHashIndex() *hashIndex {
	return &hashIndex{mu: new(sync.RWMutex), trees: make(map[string]*art.AdaptiveRadixTree)}
}

// 初始化Set 索引
func newSetIndex() *setIndex {
	return &setIndex{
		murHash: util.NewMurmur128(),
		mu:      new(sync.RWMutex),
		trees:   make(map[string]*art.AdaptiveRadixTree),
	}
}

// 初始化ZSet 索引
func newZSetIndex() *zSetIndex {
	return &zSetIndex{
		murHash: util.NewMurmur128(),
		mu:      new(sync.RWMutex),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		indexes: zset.NewSortedSet(),
	}
}

// Open 打开一个kayDB 实例，使用后必须调用Close
func Open(opts Options) (*KayDB, error) {
	// 如果目录路径不存在则创建
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 获取文件锁以防止多个进程访问同一目录
	lockPath := filepath.Join(opts.DBPath, lockFileName)
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	// 实例化db
	db := &KayDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		fileLock:         lockGuard,
		opts:             opts,
		setIndex:         newSetIndex(),
		listIndex:        newListIndex(),
		hashIndex:        newHashIndex(),
		strIndex:         newStrsIndex(),
		zSetIndex:        newZSetIndex(),
	}

	// 初始化discard文件
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// 从磁盘加载日志文件
	if err := db.loadLogFiles(); err != nil {
		return nil, err
	}

	// 从日志文件中加载索引
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// 处理日志文件的垃圾回收（merge文件）
	go db.handleLogFileGC()
	return db, err
}

// Close 关闭数据库并保存相对配置
func (db *KayDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 释放文件锁
	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
	// 关闭并同步活跃文件
	for _, activeFile := range db.activeLogFiles {
		_ = activeFile.Close()
	}
	// 关闭并同步归档文件
	for _, archived := range db.archivedLogFiles {
		for _, archivedFile := range archived {
			_ = archivedFile.Sync()
			_ = archivedFile.Close()
		}
	}
	// 关闭discard通道
	for _, dis := range db.discards {
		dis.closeChan()
	}
	// 修改文件关闭状态
	atomic.StoreUint32(&db.closed, 1)
	// 清空索引
	db.strIndex = nil
	db.listIndex = nil
	db.hashIndex = nil
	db.strIndex = nil
	db.zSetIndex = nil
	return nil
}

// 获取活跃日志文件
func (db *KayDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

// 获取归档日志文件
func (db *KayDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}

// 往日志文件中写入entry
func (db *KayDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	// 初始化日志文件
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	// 获取活跃日志文件
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}

	// 编码entry
	opts := db.opts
	entBuf, entSize := logfile.EncodeEntry(ent)
	// 如果写入数组大于日志文件的阈值大小，将活跃文件保存到归档文件中，再新建一个活跃文件
	if activeLogFile.WriteAt+int64(entSize) > opts.LogFileSizeThreshold {
		// 将活跃日志文件的当前内容提交到稳定存储
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		// 将旧日志文件保存在归档文件中
		activeFileFid := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileFid] = activeLogFile
		// 打开新的日志文件
		ftype, ioType := logfile.FileType(dataType), logfile.IOType(opts.IOType)
		file, err := logfile.OpenLogFile(opts.DBPath, activeFileFid+1, opts.LogFileSizeThreshold, ftype, ioType)
		if err != nil {
			// 记得解锁
			db.mu.Unlock()
			return nil, err
		}

		// 设置discard文件的total，将新的日志文件作为活跃日志文件
		db.discards[dataType].setTotal(file.Fid, uint32(opts.LogFileSizeThreshold))
		db.activeLogFiles[dataType] = file
		activeLogFile = file
		db.mu.Unlock()
	}
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// 写入entry和同步（如果需要）
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

// 初始化日志文件
// 如果活跃文件不为空则不用初始化日志文件，直接返回
func (db *KayDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 如果活跃文件不为空则不用初始化日志文件，直接返回
	if db.activeLogFiles[dataType] != nil {
		return nil
	}

	// 获取fileType和ioType，创建文件
	fType, ioType := logfile.FileType(dataType), logfile.IOType(db.opts.IOType)
	file, err := logfile.OpenLogFile(db.opts.DBPath, logfile.InitialLogFileId, db.opts.LogFileSizeThreshold, fType, ioType)
	if err != nil {
		return err
	}

	// 设置discard的total
	db.discards[dataType].setTotal(file.Fid, uint32(db.opts.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = file
	return nil
}

// 初始化discard 文件
func (db *KayDB) initDiscard() error {
	// 获取文件路径
	path := filepath.Join(db.opts.DBPath, discardFilePath)
	// 文件夹若不存在则创建
	if !util.PathExist(path) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		// 遍历拼接文件名，并创建discard 文件
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		dis, err := newDiscard(path, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		// 创建后的discard文件映射到map中
		discards[i] = dis
	}
	// 赋值给全局的discards
	db.discards = discards
	return nil
}

// hash结构与zSet结构编码key,返回的buf结构
// [编码后的len(key)  编码后的len(subKey) key  subKey]
func (db *KayDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	// 将key的长度与filed的长度编码写入到header
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)

	if length > 0 {
		// 若key与filed不为空，将key和filed追加
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		return buf
	}
	return header[:index]
}

// hash结构与zSet结构解码key，返回key, filed
func (db *KayDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	// 解码len(key)
	keySize, i := binary.Varint(key[index:])
	index += i
	// 解码len(filed)
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}

// 从磁盘加载日志文件
func (db *KayDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 返回dirname指定的目录信息的有序列表
	fileInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}
	// fidMap 启动时加载日志入内存
	fidMap := make(map[DataType][]uint32)
	for _, fileInfo := range fileInfos {
		// 处理文件前缀为log.
		if strings.HasPrefix(fileInfo.Name(), logfile.FilePrefix) {
			// 按 "." 切割，拿到文件id
			splitName := strings.Split(fileInfo.Name(), ".")
			fid, err := strconv.Atoi(splitName[2])
			if err != nil {
				return err
			}
			// 将FileType转为DataType，追加入fidMap中
			typ := DataType(logfile.FileTypesMap[splitName[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}
	// 加入全局的fidMap
	db.fidMap = fidMap

	// 遍历fidMap,加载日志文件
	for dataType, fids := range fidMap {
		// 初次加载
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// 按顺序加载日志
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		opts := db.opts
		// 遍历fids,处理单个日志
		for i, fid := range fids {
			// 获取fileType和ioType，打开现有的日志文件
			fType, ioType := logfile.FileType(dataType), logfile.IOType(opts.IOType)
			file, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, fType, ioType)
			if err != nil {
				return err
			}
			// 最新的是活跃日志文件
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = file
			} else {
				// 其他是归档日志文件
				db.archivedLogFiles[dataType][fid] = file
			}
		}
	}
	return nil
}

// 发送discard文件，保存着旧的entry 大小
func (db *KayDB) sendDiscard(oldValue interface{}, updated bool, dataType DataType) {
	if !updated || oldValue == nil {
		return
	}
	node, _ := oldValue.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	// 发送discard文件
	select {
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard chan failed")
	}
}

// 日志文件垃圾收集
func (db *KayDB) handleLogFileGC() {
	// 未设置定时GC
	if db.opts.LogFileGCInterval <= 0 {
		return
	}
	// 创建一个通道，接收停止信号
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	// 创建一个定时器，每到设置的时间就发送信号到channel
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()
	for {
		select {
		// 定时器channel接收到信号，执行GC
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warn("log file gc is running, skip it")
				return
			}
			// 遍历5种数据结构类型文件
			for dType := String; dType < logFileTypeNum; dType++ {
				// 开启协程执行gc
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1, db.opts.LogFileGCRatio)
					if err != nil {
						logger.Errorf("log file gc err,dataType: [%v], err: [%v]", dataType, err)
					}
				}(dType)
			}
			// 监听信号，停止GC
		case <-quitSig:
			return
		}
	}
}

// 执行日志文件垃圾收集 specifiedFid为指定的文件id，-1代表全部
func (db *KayDB) doRunGC(dataType DataType, specifiedFid int, gcRatio float64) error {
	// 修改gc的状态
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()

		// 获取记录的索引节点
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// 重写entry
			valuePos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// 修改索引节点
			if err = db.updateIndexTree(db.strIndex.idxTree, valuePos, ent, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()

		var listKey = ent.Key
		// 如果不是list meta类型，则需要解码获取真正的key
		if ent.Type != logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(listKey)
		}
		// key不存在则返回
		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		// 获取记录的索引节点
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(listKey)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// 重写entry
			pos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			// 修改索引
			if err = db.updateIndexTree(idxTree, pos, ent, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()

		// 解码hash key
		key, field := db.decodeKey(ent.Key)
		// key不存在则返回
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]

		// 获取记录的索引节点
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// 重写entry
			pos, err := db.writeLogEntry(ent, Hash)
			if err != nil {
				return err
			}
			// 修改索引
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, entrySize := logfile.EncodeEntry(entry)
			pos.entrySize = entrySize
			if err = db.updateIndexTree(idxTree, pos, entry, false, Hash); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()

		// key 不存在则返回
		if db.setIndex.trees[string(ent.Key)] == nil {
			return nil
		}
		idxTree := db.setIndex.trees[string(ent.Key)]
		// 将member写入hash，并进行hash
		if err := db.setIndex.murHash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.setIndex.murHash.EncodeSum128()
		db.setIndex.murHash.Reset()

		// 获取记录的索引节点
		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// 重写entry
			pos, err := db.writeLogEntry(ent, Set)
			if err != nil {
				return err
			}
			// 修改索引
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, entrySize := logfile.EncodeEntry(entry)
			pos.entrySize = entrySize
			if err = db.updateIndexTree(idxTree, pos, entry, false, Set); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zSetIndex.mu.Lock()
		defer db.zSetIndex.mu.Unlock()

		// 解码zSet key
		key, _ := db.decodeKey(ent.Key)
		// key不存在则返回
		if db.zSetIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.zSetIndex.trees[string(key)]

		// 将member写入hash，并进行hash
		if err := db.zSetIndex.murHash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.zSetIndex.murHash.EncodeSum128()
		db.zSetIndex.murHash.Reset()

		// 获取索引节点
		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// 重写entry
			pos, err := db.writeLogEntry(ent, ZSet)
			if err != nil {
				return err
			}
			// 修改索引
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, entrySize := logfile.EncodeEntry(entry)
			pos.entrySize = entrySize
			if err = db.updateIndexTree(idxTree, pos, entry, false, ZSet); err != nil {
				return err
			}
		}
		return nil
	}
	// 获取活跃文件
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return ErrLogFileNotFound
	}
	// 将最近写入的数据的文件系统内存副本刷新到磁盘
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}
	// 从discard中获取需要压缩的文件列表
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}

	// 遍历处理需要压缩的文件
	for _, fid := range ccl {
		// 如果需要处理特定文件，不是则跳过
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			continue
		}
		// 如果归档文件为空，则跳过
		archivedLogFile := db.getArchivedLogFile(dataType, fid)
		if archivedLogFile == nil {
			continue
		}

		var offset int64
		for {
			// 遍历读取归档文件的entry
			entry, size, err := archivedLogFile.ReadLogEntry(offset)
			if err != nil {
				// 读取结束
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			// 保存并更新offset
			var off = offset
			offset += size

			// 如果entry是删除类型、过期记录，则跳过
			if entry.Type == logfile.TypeDelete {
				continue
			}
			now := time.Now().Unix()
			if entry.ExpiredAt != 0 && entry.ExpiredAt <= now {
				continue
			}

			// 重写日志
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = maybeRewriteStrs(archivedLogFile.Fid, off, entry)
			case List:
				rewriteErr = maybeRewriteList(archivedLogFile.Fid, off, entry)
			case Hash:
				rewriteErr = maybeRewriteHash(archivedLogFile.Fid, off, entry)
			case Set:
				rewriteErr = maybeRewriteSets(archivedLogFile.Fid, off, entry)
			case ZSet:
				rewriteErr = maybeRewriteZSets(archivedLogFile.Fid, off, entry)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}
		// 删除旧的日志文件
		db.mu.Lock()
		// 删除内存中的map
		delete(db.archivedLogFiles[dataType], fid)
		_ = archivedLogFile.Delete()
		db.mu.Unlock()
		// 清除内存中丢弃文件的map
		db.discards[dataType].clear(fid)
	}
	return nil
}
