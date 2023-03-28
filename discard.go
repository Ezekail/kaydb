package kaydb

import (
	"encoding/binary"
	"errors"
	"github.com/Ezekail/kaydb/ioselector"
	"github.com/Ezekail/kaydb/logfile"
	"github.com/Ezekail/kaydb/logger"
	"io"
	"path/filepath"
	"sort"
	"sync"
)

const (
	discardRecordSize = 12
	// 8kb, 文件一般能包含682条记录
	discardFileSize int64 = 2 << 12
	discardFileName       = "discard"
)

// ErrDiscardNoSpace 无法为丢弃文件分配足够的空间
var ErrDiscardNoSpace = errors.New("not enough space can be allocated fro the discard file ")

type discard struct {
	sync.Mutex
	once     *sync.Once
	valChan  chan *indexNode       // 丢弃文件的接收通道
	file     ioselector.IOSelector // fileIO 和 mMap的选择器
	freeList []int64               // 包含可分配的文件偏移量
	location map[uint32]int64      // 每个fid的位置（偏移量）
}

// 创建discard文件
func newDiscard(path, name string, buffSize int) (*discard, error) {
	fName := filepath.Join(path, name)
	// 创建mmap选择器
	file, err := ioselector.NewMMapIOSelector(fName, discardFileSize)
	if err != nil {
		return nil, err
	}

	var offset int64
	var freeList []int64
	location := make(map[uint32]int64)
	for {
		// 只读取fid 和 total
		buf := make([]byte, 8)
		if _, err := file.Read(buf, offset); err != nil {
			// 读取到文件末端
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		// 小端字节序解码
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && total == 0 {
			// 追加入可分配的文件偏移量
			freeList = append(freeList, offset)
		} else {
			// 存入内存location中，每个fid的偏移量
			location[fid] = offset
		}
		// 更新offset
		offset += discardRecordSize
	}
	d := &discard{
		valChan:  make(chan *indexNode, buffSize),
		once:     new(sync.Once),
		file:     file,
		freeList: freeList,
		location: location,
	}
	// 开启协程，监听更新
	go d.listenUpdates()
	return d, nil
}

// 将最近写入的数据的文件系统内存副本刷新到磁盘
func (d *discard) sync() error {
	return d.file.Sync()
}

// 关闭文件，使其无法用于I/O
func (d *discard) close() error {
	return d.file.Close()
}

// CCL: compaction candidate list 压缩候选列表
// 迭代并查找包含最多丢弃数据的文件，最多有682条记录，不必担心性能
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	d.Lock()
	defer d.Unlock()

	var offset int64
	var ccl []uint32
	for {
		// 读取discard
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			// 读到文件的末端
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		// 更新offset
		offset += discardRecordSize

		// 小端字节序解码
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		discard := binary.LittleEndian.Uint32(buf[8:12])

		var curRatio float64
		if total != 0 && discard != 0 {
			// 计算当前的比例，total为文件的阈值
			curRatio = float64(discard) / float64(total)
		}
		// 如果日志文件中丢弃的数据超过此比率，则可以将其拾取以进行压缩（垃圾收集）
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}
	}
	// 按升序排序，确保先压缩旧文件
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

// 监听更新行为
func (d *discard) listenUpdates() {
	for {
		select {
		// 收到通道的值
		case idxNode, ok := <-d.valChan:
			if !ok {
				// 关闭文件
				if err := d.file.Close(); err != nil {
					logger.Errorf("close discard file err: %v", err)
				}
				return
			}
			// 发送的旧文件id和entrySize
			d.incrDiscard(idxNode.fid, idxNode.entrySize)
		}
	}
}

// 关闭通道
func (d *discard) closeChan() {
	d.once.Do(func() {
		close(d.valChan)
	})
}

// 设置total
func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	// 如果存在则直接返回
	if _, ok := d.location[fid]; ok {
		return
	}

	// 为fid分配offset
	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	// 编码数据，设置fid和total
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)

	// 写入文件
	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err: %v", err)
		return
	}
}

// 清除location的数据
func (d *discard) clear(fid uint32) {
	// 将无效大小记录在日志文件中
	d.incr(fid, -1)

	d.Lock()
	// 清除location的数据 并回收freeList的空间
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
	d.Unlock()
}

// 增加discard记录
func (d *discard) incrDiscard(fid uint32, delta int) {
	if delta > 0 {
		d.incr(fid, delta)
	}
}

// 丢弃文件记录的格式:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
func (d *discard) incr(fid uint32, delta int) {
	d.Lock()
	defer d.Unlock()

	// 为fid分配offset
	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	var buf []byte
	// 创建一个通道，当key更新或删除时，发送旧的entry 大小
	// entry大小将保存在丢弃文件中，将无效大小记录在日志文件中，并在日志文件gc运行时使用
	if delta > 0 {
		// 只读丢弃文件的大小
		buf = make([]byte, 4)
		offset += 8
		if _, err := d.file.Read(buf, offset); err != nil {
			logger.Errorf("incr value in discard err: %v ", err)
			return
		}

		// 解码
		v := binary.LittleEndian.Uint32(buf)
		// 加上旧entry大小后编码
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		// 将无效大小记录在日志文件
		buf = make([]byte, discardRecordSize)
	}
	// 写入文件
	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err: %v", err)
		return
	}
}

// 分配offset，调用之前必须保持锁定状态
func (d *discard) alloc(fid uint32) (int64, error) {
	// 若已分配则返回
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	// 查看是否存在可分配的文件偏移量
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}

	// 从freeList中取出分配
	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}
