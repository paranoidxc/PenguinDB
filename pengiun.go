package penguinDB

import (
	"github.com/paranoidxc/PenguinDB/face"
	"github.com/paranoidxc/PenguinDB/impl/index"
	"github.com/paranoidxc/PenguinDB/impl/store"
	"github.com/paranoidxc/PenguinDB/wal"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options     Options
	mu          *sync.RWMutex
	index       face.Indexer
	activeFile  *wal.DataFile
	isInitial   bool
	bytesWrite  uint
	reclaimSize int64
}

func Open(options Options) (*DB, error) {
	var isInitial bool
	if _, err := os.Stat(options.PersistentDir); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.PersistentDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	files, err := os.ReadDir(options.PersistentDir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		isInitial = true
	}

	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		index:     index.MakeSyncDict(),
		isInitial: isInitial,
	}

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 读取数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 从数据文件中读取索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

// 从磁盘加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.PersistentDir)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), wal.StoreFileNameSuffix) {
			// 0000.data 分隔
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录肯被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小大大依次加载
	sort.Ints(fileIds)
	//db.fileIds = fileIds

	// 遍历每个文件的id，打开对应的数据文件

	for i, fid := range fileIds {
		ioType := store.StandardFIO
		dataFile, err := wal.OpenDataFile(db.options.PersistentDir, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个，id是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的数据文件
			//db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFile() error {
	updateIndex := func(key []byte, typ face.LogEntryType, pos *face.LogEntryPos) {
		var oldPos *face.LogEntryPos
		if typ == wal.LogEntryTypeDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Set(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	var dataFile *wal.DataFile
	dataFile = db.activeFile
	fileID := db.activeFile.FileId

	var offset int64 = 0
	for {
		logRecord, size, err := dataFile.ReadLogEntry(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 构造内存索引并保存
		logRecordPos := &face.LogEntryPos{Fid: fileID, Offset: offset, Size: uint32(size)}

		// 解析 Key，拿到事务序列号
		realKey := logRecord.Key
		updateIndex(realKey, logRecord.Type, logRecordPos)

		// 递增 offset， 下一次直接从新的位置读取
		offset += size
	}

	db.activeFile.Offset = offset

	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭内存索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Set(key []byte, value []byte) (interface{}, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 构造 kv 结构体
	logEntry := &face.LogEntry{
		Key:   key,
		Value: value,
		Type:  wal.LogEntryTypeNormal,
	}

	// 写入文件
	entryPos, err := db.appendLogEntryWithLock(logEntry)
	if err != nil {
		return nil, err
	}

	// 写入内存索引
	if oldEntryPos := db.index.Set(key, entryPos); oldEntryPos != nil {
		db.reclaimSize += int64(oldEntryPos.Size)
	}

	return nil, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	logEntryPos := db.index.Get(key)
	if logEntryPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取value
	return db.getValueByPosition(logEntryPos)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if entryPos := db.index.Get(key); entryPos == nil {
		return nil
	}

	// 构造 logRecord 信息，标识其是被删除的
	logEntry := &face.LogEntry{
		Key:  key,
		Type: wal.LogEntryTypeDeleted,
	}
	// 写入到数据文件中
	entryPos, err := db.appendLogEntryWithLock(logEntry)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64(entryPos.Size)

	// 从内存索引中删除
	oldEntryPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	if oldEntryPos != nil {
		db.reclaimSize += int64(oldEntryPos.Size)
	}
	return nil
}

func (db *DB) All() (interface{}, error) {
	return nil, nil
}

func (db *DB) Merge() (interface{}, error) {
	return nil, nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) getValueByPosition(logEntryPos *face.LogEntryPos) ([]byte, error) {
	var df *wal.DataFile
	df = db.activeFile
	if df == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数
	logEntry, _, err := df.ReadLogEntry(logEntryPos.Offset)
	if err != nil {
		return nil, err
	}

	if logEntry.Type == wal.LogEntryTypeDeleted {
		return nil, ErrKeyNotFound
	}

	return logEntry.Value, nil
}

func (db *DB) appendLogEntryWithLock(entry *face.LogEntry) (*face.LogEntryPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogEntry(entry)
}

func (db *DB) appendLogEntry(entry *face.LogEntry) (*face.LogEntryPos, error) {
	// 写入数据编码
	encodeEntry, size := wal.EncodeLogEntry(entry)
	offset := db.activeFile.Offset

	if err := db.activeFile.Write(encodeEntry); err != nil {
		return nil, err
	}

	// 内存索引信息
	pos := &face.LogEntryPos{
		Fid:    db.activeFile.FileId,
		Offset: offset,
		Size:   uint32(size),
	}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialField uint32 = 0
	if db.activeFile != nil {
		initialField = db.activeFile.FileId + 1
	}

	dataFile, err := wal.OpenDataFile(db.options.PersistentDir, initialField, store.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}
