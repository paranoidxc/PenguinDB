package index

import (
	"github.com/paranoidxc/PenguinDB/face"
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func (dict *SyncDict) Iterator(reverse bool) face.Iterator {
	return nil
}

func (dict *SyncDict) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (dict *SyncDict) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (dict *SyncDict) Next() {
	//TODO implement me
	panic("implement me")
}

func (dict *SyncDict) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (dict *SyncDict) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (dict *SyncDict) Value() *face.LogEntryPos {
	//TODO implement me
	panic("implement me")
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Set(key []byte, pos *face.LogEntryPos) *face.LogEntryPos {
	k := string(key)
	old, existed := dict.m.Load(k)
	item := face.Item{Key: key, Pos: pos}
	dict.m.Store(k, item)
	if existed {
		return old.(face.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Get(key []byte) *face.LogEntryPos {
	val, ok := dict.m.Load(string(key))
	if ok {
		return val.(face.Item).Pos
	}
	return nil
}

func (dict *SyncDict) Delete(key []byte) (*face.LogEntryPos, bool) {
	k := string(key)
	old, existed := dict.m.Load(k)
	if existed {
		dict.m.Delete(k)
		return old.(face.Item).Pos, true
	}
	return nil, false
}

func (dict *SyncDict) Keys() [][]byte {
	result := make([][]byte, dict.Size())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = []byte(key.(string))
		i++
		return true
	})

	return result
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
