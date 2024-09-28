package data

import (
	"fmt"
	"github.com/paranoidxc/PenguinDB/impl/store"
	"github.com/paranoidxc/PenguinDB/interface/storeer"
	"path/filepath"
)

const (
	StoreFileNameSuffix = ".data"
)

type DataFile struct {
	FileId    uint32
	Offset    int64
	IoManager storeer.IOMgr
}

func (d DataFile) Read(bytes []byte, i int64) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (d DataFile) Write(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (d DataFile) Sync() error {
	//TODO implement me
	panic("implement me")
}

func (d DataFile) Close() error {
	//TODO implement me
	panic("implement me")
}

func (d DataFile) Size() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func OpenDataFile(dirPath string, fileId uint32, ioType store.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+StoreFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType store.FileIOType) (*DataFile, error) {
	ioManager, err := store.NewFileIOMgr(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		Offset:    0,
		IoManager: ioManager,
	}, nil
}
