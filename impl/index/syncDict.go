package index

import (
	"sync"

	"github.com/paranoidxc/PenguinDB/wal"
)

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Set(key []byte, pos *wal.LogEntryPos) *wal.LogEntryPos {
	k := string(key)
	old, existed := dict.m.Load(k)
	item := wal.Item{Key: key, Pos: pos}
	dict.m.Store(k, item)
	if existed {
		return old.(wal.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Get(key []byte) *wal.LogEntryPos {
	val, ok := dict.m.Load(string(key))
	if ok {
		return val.(wal.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Delete(key []byte) (*wal.LogEntryPos, bool) {
	k := string(key)
	old, existed := dict.m.Load(k)
	if existed {
		dict.m.Delete(k)
		return old.(wal.Item).Pos, true
	}
	return nil, false
}

func (dict *SyncDict) Size() int {
	length := 0
	dict.m.Range(func(key, value any) bool {
		length += 1
		return true
	})
	return length
}

func (dict *SyncDict) Close() error {
	return nil
}
