package fedis

import (
	penguinDB "github.com/paranoidxc/PenguinDB"
	"github.com/paranoidxc/PenguinDB/lib/utils"

	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
	db *penguinDB.DB
}

// NewRedisDataStructure 初始化Redis 数据结构服务
func NewRedisDataStructure(opts penguinDB.Options) (*RedisDataStructure, error) {
	db, err := penguinDB.Open(opts)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ========================== String 数据结构 ===========================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value : type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//调用存储接口进行写入
	_, err := rds.db.Set(key, encValue)
	return err
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

// ========================== Hash 数据结构 ===========================

func (rds *RedisDataStructure) HSet(key, filed, value []byte) (bool, error) {
	// 先查找对应的元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   filed,
	}
	encKey := hk.encode()

	//先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		exist = false
	}

	// 不存在则更新元数据
	wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)

	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err := wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// ========================== Set 数据结构 ===========================

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	//先查找是否存在
	var ok bool
	if _, err = rds.db.Get(encKey); errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		// 更新的时候 使用 write batch 保证原子性
		wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// key 存在但是没有数据
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		return false, err
	}

	if errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		return false, nil
	}

	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// key 存在但是没有数据
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err := rds.db.Get(sk.encode()); errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ========================== List 数据结构 ===========================

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}

	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 构造元数据
	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	if _, err = rds.db.Set(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ========================== ZSet 数据结构 ===========================

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的 key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		return false, err
	}
	if errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据
	wb := rds.db.NewWriteBatch(penguinDB.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}

	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if errors.Is(err, penguinDB.ERR_KEY_NOT_FOUND) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
