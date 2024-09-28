package storeer

type IOMgr interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}
