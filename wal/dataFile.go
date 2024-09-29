package wal

import (
	"errors"
	"fmt"
	"github.com/paranoidxc/PenguinDB/impl/store"
	"github.com/paranoidxc/PenguinDB/interface/storeer"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("INVALID CRC VALUE, LOG ENTRY CORRUPTED")
)

const (
	StoreFileNameSuffix = ".data"
)

type DataFile struct {
	FileId    uint32
	Offset    int64
	IoManager storeer.IoStoreer
}

func (df *DataFile) Write(bytes []byte) error {
	n, err := df.IoManager.Write(bytes)
	if err != nil {
		return err
	}
	df.Offset += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func OpenDataFile(dirPath string, fileId uint32, ioType store.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+StoreFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType store.FileIOType) (*DataFile, error) {
	ioManager, err := store.NewFileIoStore(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		Offset:    0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 Header 长度已经超过了文件的长度，则只需要读取到文件的结尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解码 Header 信息
	header, headerSize := decodeLogEntryHeader(headerBuf)
	// 判断是否读取到了文件末尾，直接返回 EOF 错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 读取 Key 和 Value 信息
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var entrySize = headerSize + keySize + valueSize

	logEnry := &LogEntry{
		Type: header.entryType,
	}

	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		keyBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 取出实际的 key 和 value
		logEnry.Key = keyBuf[:keySize]
		logEnry.Value = keyBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogEntryCRC(logEnry, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logEnry, entrySize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
