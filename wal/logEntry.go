package wal

import (
	"encoding/binary"
	"github.com/paranoidxc/PenguinDB/face"
	"hash/crc32"
)

const (
	LogEntryTypeNormal face.LogEntryType = iota
	LogEntryTypeDeleted
	LogEntryTypeTxnFinished
)

// crc type keySize valueSize 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = 15

type logEntryHeader struct {
	crc       uint32
	entryType face.LogEntryType
	keySize   uint32
	valueSize uint32
}

// LogEntry 加上 头部信息
func EncodeLogEntry(entry *face.LogEntry) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = entry.Type
	var index = 5

	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(entry.Key)))
	index += binary.PutVarint(header[index:], int64(len(entry.Value)))

	var size = index + len(entry.Key) + len(entry.Value)
	encBytes := make([]byte, size)

	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], entry.Key)
	copy(encBytes[index+len(entry.Key):], entry.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// LogHeader 信息进行解码
func decodeLogEntryHeader(buf []byte) (*logEntryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logEntryHeader{
		crc:       binary.LittleEndian.Uint32(buf[:4]),
		entryType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)

	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}

func EncodeLogEntryPos(pos *face.LogEntryPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

func DecodeLogEntryPos(buf []byte) *face.LogEntryPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &face.LogEntryPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}

func getLogEntryCRC(entry *face.LogEntry, header []byte) uint32 {
	if entry == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, entry.Key)
	crc = crc32.Update(crc, crc32.IEEETable, entry.Value)

	return crc
}
