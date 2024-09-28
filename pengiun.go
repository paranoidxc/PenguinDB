package penguinDB

import (
	"github.com/paranoidxc/PenguinDB/data"
	"github.com/paranoidxc/PenguinDB/impl/index"
	"github.com/paranoidxc/PenguinDB/impl/store"
	"github.com/paranoidxc/PenguinDB/interface/indexer"
	"os"
	"sync"
)

type DB struct {
	options     Options
	mu          *sync.RWMutex
	index       indexer.Indexer
	activeFile  *data.DataFile
	isInitial   bool
	bytesWrite  uint
	reclaimSize int64
}

func Open(options Options) (*DB, error) {
	var isInitial bool
	if _, err := os.Stat(options.StorePath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(options.StorePath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	files, err := os.ReadDir(options.StorePath)
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

	return db, nil
}

func (db *DB) Set(key []byte, value []byte) (interface{}, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty

	logEntry := &data.LogEntry{
		Key:   key,
		Value: value,
		Type:  data.LogEntryTypeNormal,
	}

	// 写入文件
	pos, err := db.appendLogEntryWithLock(logEntry)
	if err != nil {
		return nil, err
	}

	// 写入内存索引
	if oldPos := db.index.Set(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	pos := db.index.Get(key)
	if pos != nil {
		return db.getValueByPosition(pos)
	}

	return nil, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return nil
	}

	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) All() (interface{}, error) {
	return nil, nil
}

func (db *DB) Merge() (interface{}, error) {
	return nil, nil
}

func (db *DB) Sync() (interface{}, error) {
	return nil, nil
}

func (db *DB) getValueByPosition(logEntryPos *data.LogEntryPos) ([]byte, error) {
	if logEntryPos != nil {

	}
	return []byte("fake data"), nil
}

func (db *DB) appendLogEntryWithLock(entry *data.LogEntry) (*data.LogEntryPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogEntry(entry)
}

func (db *DB) appendLogEntry(entry *data.LogEntry) (*data.LogEntryPos, error) {
	pos := &data.LogEntryPos{
		Fid:    db.activeFile.FileId,
		Offset: db.activeFile.Offset,
		Size:   uint32(1),
	}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialField uint32 = 0
	if db.activeFile != nil {
		initialField = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.StorePath, initialField, store.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}
