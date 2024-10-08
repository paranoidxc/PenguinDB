package face

type LogEntryType = byte

type Item struct {
	Key []byte
	Pos *LogEntryPos
}

type LogEntryPos struct {
	Fid    uint32 // 文件 id 表示将数据存储的哪个文件当中
	Offset int64  // 日志块偏移，表示将数据存储到了数据文件中的哪个位置
	Size   uint32 // 标识数据在磁盘上的大小
}

type LogEntry struct {
	Key   []byte
	Value []byte
	Type  LogEntryType
}

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogEntry
	Pos    *LogEntryPos
}

type Indexer interface {
	// Set 向索引中存储 key 对应的数值位置信息
	Set(key []byte, pos *LogEntryPos) *LogEntryPos

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *LogEntryPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*LogEntryPos, bool)

	// Size 返回索引中存在了多少条数据
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	Keys() [][]byte

	Close() error
}

// Iterator 通用的索引迭代器接口
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找第一个大于(或小于)等于的目标key，从这个key开始遍历
	Seek(key []byte)

	// Next 跳转到下一个key
	Next()

	// Valid 当前遍历的位置的
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *LogEntryPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
