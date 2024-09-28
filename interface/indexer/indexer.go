package indexer

import "github.com/paranoidxc/PenguinDB/data"

type Indexer interface {
	// Set 向索引中存储 key 对应的数值位置信息
	Set(key []byte, pos *data.LogEntryPos) *data.LogEntryPos

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogEntryPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogEntryPos, bool)

	// Size 返回索引中存在了多少条数据
	Size() int
}
