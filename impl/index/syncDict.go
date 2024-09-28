package index

import (
	"sync"

	"github.com/paranoidxc/PenguinDB/data"
)

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Set(key []byte, pos *data.LogEntryPos) *data.LogEntryPos {
	k := string(key)
	old, existed := dict.m.Load(k)
	item := data.Item{Key: key, Pos: pos}
	dict.m.Store(k, item)
	if existed {
		return old.(data.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Get(key []byte) *data.LogEntryPos {
	val, ok := dict.m.Load(string(key))
	if ok {
		return val.(data.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Delete(key []byte) (*data.LogEntryPos, bool) {
	k := string(key)
	old, existed := dict.m.Load(k)
	if existed {
		dict.m.Delete(k)
		return old.(data.Item).Pos, true
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
