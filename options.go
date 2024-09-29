package penguinDB

import "os"

type Options struct {
	PersistentDir             string
	PersistentDataFileSizeMax int64
}

var DefaultOptions = Options{
	PersistentDir: os.TempDir(),
	//PersistentDataFileSizeMax: 256 * 1024 * 1024, // 256MB
	PersistentDataFileSizeMax: 2, // 2KB
}
