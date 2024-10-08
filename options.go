package penguinDB

import "os"

type Options struct {
	PersistentDir                string
	PersistentDataFileSizeMax    int64
	PersistentDataFileMergeRatio float32
	IndexType                    IndexerType
}

var DefaultOptions = Options{
	PersistentDir: os.TempDir(),
	//PersistentDataFileSizeMax: 256 * 1024 * 1024, // 256MB
	PersistentDataFileSizeMax:    2, // 2KB
	PersistentDataFileMergeRatio: 0.5,
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchSize uint

	// 提交事务的时候，是否进行可持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	Dict IndexerType = iota + 1
	Btree
	ART
	BPlusTree
)

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 10000,
	SyncWrites:   true,
}
