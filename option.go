package kaydb

import "time"

// DataIndexMode 数据索引模式
type DataIndexMode int

const (
	// KeyValueMemMode key和value都在内存中，该模式下读取操作非常快
	// 因为没有查找磁盘，只需从内存中相应的数据结构中取出值
	// 该模式很适合值相对较小的场景
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode 只有key在内存中，获取值时需查找磁盘
	// 因为值以日志文件形式存储在磁盘中
	KeyOnlyMemMode
)

// IOType 表示不同类型的文件io：FileIO（标准文件io）和MMap（内存映射）
type IOType int8

const (
	// FileIO 标准文件io
	FileIO IOType = iota
	// MMap 内存映射
	MMap
)

// Options 开启数据库的选项
type Options struct {
	// 数据库路径，如果不存在则自动创建
	DBPath string

	// 数据库索引模式，现在支持KeyValueMemMode 和 KeyOnlyMemMode
	// 请注意，此模式仅适用于kv对，而不是List、Hash、Set和ZSet
	// 默认值为KeyOnlyMemMode
	IndexMode DataIndexMode

	// 文件读写IO类型，支持标准文件io和内存映射
	// 默认为标准文件io
	IOType IOType

	// 是否将写入从OS缓冲区缓存同步到实际磁盘
	// 如果为false，若机器崩溃，那么最近的一些写入可能会丢失
	// 请注意，如果只是进程崩溃（而机器没有崩溃），则不会丢失任何写入
	// 默认为false
	Sync bool

	// 后台goroutine将根据间隔定期执行日志文件垃圾收集
	// 它将选择符合GC条件的日志文件，然后逐个重写有效数据
	// 默认为8小时
	LogFileGCInterval time.Duration

	// 如果日志文件中丢弃的数据超过此比率，则可以将其拾取以进行压缩（垃圾收集）
	// 如果有很多文件达到这个比例，我们将逐个选择最高的
	// 建议比率为0.5，可压缩一半的文件
	// 默认为0.5
	LogFileGCRatio float64

	// 每个日志文件的阈值大小，如果达到阈值，活动日志文件将关闭
	// 重点！！！此选项必须设置为与第一次启动时相同的值
	// 默认为512MB
	LogFileSizeThreshold int64

	// 创建一个通道，当key更新或删除时，发送旧的entry 大小
	// entry大小将保存在丢弃文件中，将无效大小记录在日志文件中，并在日志文件gc运行时使用
	// 此选项表示该通道的大小
	// 如果出现“send discard chan fail”之类的错误，可以增加此选项以避免
	DiscardBufferSize int
}

// DefaultOptions 以默认选项打开KayDB
func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path, // 拼接路径+数据库名
		IndexMode:            KeyOnlyMemMode,
		IOType:               FileIO,
		Sync:                 false,
		LogFileGCInterval:    8 * time.Hour,
		LogFileGCRatio:       0.5,
		LogFileSizeThreshold: 512 << 20,
		DiscardBufferSize:    8 << 20,
	}
}
