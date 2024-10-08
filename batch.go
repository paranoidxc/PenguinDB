package penguinDB

import (
	"encoding/binary"
	"github.com/paranoidxc/PenguinDB/face"
	"github.com/paranoidxc/PenguinDB/wal"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子批量写数据、保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*face.LogEntry // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*face.LogEntry),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ERR_KEY_IS_EMPTY
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	logRecord := &face.LogEntry{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ERR_KEY_IS_EMPTY
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord
	logRecord := &face.LogEntry{Key: key, Type: wal.LogEntryTypeDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务 将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 不存在缓存的数据 直接返回
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 数据量过大 返回错误
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
		return ERR_EXCEED_MAX_BATCH_NUM
	}

	// 加锁保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	positions := make(map[string]*face.LogEntryPos)

	// 开始去写数据
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogEntry(&face.LogEntry{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &face.LogEntry{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: wal.LogEntryTypeTxnFinished,
	}
	if _, err := wb.db.appendLogEntry(finishedRecord); err != nil {
		return err
	}

	// 根据配置去进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新对应的内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *face.LogEntryPos
		if record.Type == wal.LogEntryTypeNormal {
			oldPos = wb.db.index.Set(record.Key, pos)
		}
		if record.Type == wal.LogEntryTypeDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存的数据
	wb.pendingWrites = make(map[string]*face.LogEntry)

	return nil
}

// key + Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 Key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
