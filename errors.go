package penguinDB

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("THE KEY IS EMPTY")
	ErrIndexUpdateFailed      = errors.New("FAILED TO UPDATE INDEX")
	ErrKeyNotFound            = errors.New("KEY NOT FOUND IN DATABASE")
	ErrDataFileNotFound       = errors.New("DATA FILE IS NOT FOUND")
	ErrDataDirectoryCorrupted = errors.New("THE DATABASE DIRECTORY MAYBE CORRUPTED")
	ErrExceedMaxBatchNum      = errors.New("EXCEED THE MAX BATCH NUM")
	ErrMergeIsProgress        = errors.New("MERGE IS IN PROGRESS, TRY AGAIN LATER")
	ErrDatabaseIsUsing        = errors.New("THE DATABASE DIRECTORY IS USED BY ANOTHER PROCESS")
	ErrMergeRatioUnreached    = errors.New("THE MERGE RATIO DO NOT REACH THE OPTION")
	ErrNoEnoughSpaceForMerge  = errors.New("NO ENOUGH DISK SPACE FOR MERGE")
	ErrBackupDirIsEmpty       = errors.New("BACKUP DIRECTORY IS EMPTY")
)
