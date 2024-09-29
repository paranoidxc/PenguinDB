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

type Indexer interface {
	// Set 向索引中存储 key 对应的数值位置信息
	Set(key []byte, pos *LogEntryPos) *LogEntryPos

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *LogEntryPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*LogEntryPos, bool)

	// Size 返回索引中存在了多少条数据
	Size() int

	Close() error
}
