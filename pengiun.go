package penguinDB

import (
	"github.com/paranoidxc/PenguinDB/face"
	"github.com/paranoidxc/PenguinDB/impl/index"
	"github.com/paranoidxc/PenguinDB/impl/store"
	"github.com/paranoidxc/PenguinDB/lib/logger"
	"github.com/paranoidxc/PenguinDB/lib/utils"
	"github.com/paranoidxc/PenguinDB/wal"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	fileLockName     = "flock"
	mergeDirName     = "-merge"
	mergeFinishedKye = "merge.finished"
)

type DB struct {
	options     Options
	closed      bool
	isMerging   bool
	mu          *sync.RWMutex
	index       face.Indexer
	activeFile  *wal.DataFile
	fileIds     []int
	olderFiles  map[uint32]*wal.DataFile
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
		options:    options,
		mu:         new(sync.RWMutex),
		index:      index.MakeSyncDict(),
		olderFiles: make(map[uint32]*wal.DataFile),
		isInitial:  isInitial,
	}

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
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

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成文件 判断 merge 是否处理完毕
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == wal.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除对应的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := wal.GetDataFileName(db.options.PersistentDir, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.PersistentDir, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := wal.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	logEntry, _, err := mergeFinishedFile.ReadLogEntry(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(logEntry.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
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
				logger.Error("load data file error: ", err)
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小大大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

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
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFile() error {
	if len(db.fileIds) == 0 {
		return nil
	}

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

	// 遍历文件 加载索引
	for i, fid := range db.fileIds {
		var fileID = uint32(fid)
		var dataFile *wal.DataFile

		if fileID == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileID]
		}

		var offset int64 = 0
		for {
			logEntry, size, err := dataFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logEntryPos := &face.LogEntryPos{Fid: fileID, Offset: offset, Size: uint32(size)}

			// 解析 Key，拿到事务序列号
			realKey := logEntry.Key
			updateIndex(realKey, logEntry.Type, logEntryPos)

			// 递增 offset， 下一次直接从新的位置读取
			offset += size
		}

		if i == len(db.fileIds)-1 {
			db.activeFile.Offset = offset
		}
	}

	return nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDbClosed
	}
	db.closed = true

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

	if db.closed {
		return nil, ErrDbClosed
	}

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

func (db *DB) Keys() [][]byte {
	keys := make([][]byte, db.index.Size())
	_, ok := db.index.(*index.SyncDict)
	if ok {
		keys = db.index.Keys()
	} else {
		iterator := db.index.Iterator(false)
		defer iterator.Close()
		var idx int
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			keys[idx] = iterator.Key()
			idx += 1
		}
	}
	return keys
}

func (db *DB) getMergePath() string {
	dir := filepath.Dir(filepath.Clean(db.options.PersistentDir))
	base := filepath.Base(db.options.PersistentDir)

	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	if db.closed {
		db.mu.Unlock()
		return ErrDbClosed
	}

	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	totalSize, err := utils.DirSize(db.options.PersistentDir)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if float32(db.reclaimSize)/float32(totalSize) < db.options.PersistentDataFileMergeRatio {
		//db.mu.Unlock()
		//return ErrMergeRatioUnreached
	}

	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	nonMergeFileId := db.activeFile.FileId
	var mergeFilesPoint []*wal.DataFile
	for _, file := range db.olderFiles {
		mergeFilesPoint = append(mergeFilesPoint, file)
	}
	db.mu.Unlock()

	// 按文件号从小大大排序
	sort.Slice(mergeFilesPoint, func(i, j int) bool {
		return mergeFilesPoint[i].FileId < mergeFilesPoint[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过 merge 将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的临时 db 实例
	mergeOptions := db.options
	mergeOptions.PersistentDir = mergePath
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFilesPoint {
		var offset int64 = 0
		for {
			logEntry, size, err := dataFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			realKey := logEntry.Key
			logEntryPos := db.index.Get(realKey)
			// 和内存中的索引位置进行比较。如果有效则重写
			if logEntryPos != nil &&
				logEntryPos.Fid == dataFile.FileId &&
				logEntryPos.Offset == offset {
				// 不需要使用事务序列号 清除事务标记
				logEntry.Key = realKey
				_, err := mergeDB.appendLogEntry(logEntry)
				if err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}

	//  保证持久化
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	// 写标识 merge 完成的文件
	mergeFinishedFile, err := wal.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &face.LogEntry{
		Key:   []byte(mergeFinishedKye),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := wal.EncodeLogEntry(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDbClosed
	}

	return db.activeFile.Sync()
}

func (db *DB) Backup(dir string) error {
	if len(dir) == 0 {
		return ErrBackupDirIsEmpty
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDbClosed
	}

	return utils.CopyDir(db.options.PersistentDir, dir, []string{fileLockName})
}

func (db *DB) getValueByPosition(logEntryPos *face.LogEntryPos) ([]byte, error) {
	var df *wal.DataFile
	if db.activeFile.FileId == logEntryPos.Fid {
		df = db.activeFile
	} else {
		df = db.olderFiles[logEntryPos.Fid]
	}
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
	if db.closed {
		return nil, ErrDbClosed
	}
	return db.appendLogEntry(entry)
}

func (db *DB) appendLogEntry(entry *face.LogEntry) (*face.LogEntryPos, error) {
	// 写入数据编码
	encodeEntry, size := wal.EncodeLogEntry(entry)

	// 判断是否要开启新的文件
	if db.activeFile.Offset+size > db.options.PersistentDataFileSizeMax {
		// 先将当前活跃文件进行持久化，保证已有的数据持久到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

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
