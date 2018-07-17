package main

import (
	"os"
	"log"
	"fmt"

	"compress/gzip"
	"encoding/gob"

	"github.com/pkg/errors"
	"github.com/gomodule/redigo/redis"
	"crypto/sha256"
	"github.com/spaolacci/murmur3"
)

/*
初始化时，需要一个长度为n比特的数组，每个比特位初始化为0，需要k个hash函数，每个函数可以把key散列到数组的k个位置。
当某个key加入集合时，用k个hash函数计算出k个散列值，并把数组中对应的比特位置为1；当判断某个key是否在集合时，用k个hash函数计算出k个散列值，并查询数组中对应的比特位，如果所有的比特位都是1，认为在集合中。
但是此时可能存在误报的情况，即所有这些位置是在其他元素插入过程中被偶然置为1了，导致了一次“误报”。
 */

/*
interface to explode functions such as
put bytes,put string, find byte, find string and close
 */
type BloomFilter interface {
	Put([]byte)
	PutString(string)

	Has([]byte) bool
	HasString(string) bool

	Close()
}

/*
struct in mem, file and redis
 */
type FileBloomFilter struct {
	*MemoryBloomFilter
	target string
}

type MemoryBloomFilter struct {
	k  uint
	bs BitSets
}

type RedisBloomFilter struct {
	cli redis.Conn
	n   uint
	k   uint
}

/*
key hash
 */
func HashData(data []byte, seed uint) uint {
	shaData := sha256.Sum256(data)
	data = shaData[:]
	m := murmur3.New64WithSeed(uint32(seed))
	m.Write(data)
	return uint(m.Sum64())
}

/*
create a memory bloom filter
 */
func NewMemoryBloomFilter(n uint, k uint) *MemoryBloomFilter {
	return &MemoryBloomFilter{
		k:  k,
		bs: NewBitSets(n),
	}
}

/*
compute hash value with capacity of k
 */
func (filter *MemoryBloomFilter) Put(data []byte) {
	l := uint(len(filter.bs))
	for i := uint(0); i < filter.k; i++ {
		filter.bs.Set(HashData(data, i) % l)
	}
}

// Put 添加一条string记录
func (filter *MemoryBloomFilter) PutString(data string) {
	filter.Put([]byte(data))
}

// Has 推测记录是否已存在
func (filter *MemoryBloomFilter) Has(data []byte) bool {
	l := uint(len(filter.bs))

	for i := uint(0); i < filter.k; i++ {
		if !filter.bs.IsSet(HashData(data, i) % l) {
			return false
		}
	}

	return true
}

// Has 推测记录是否已存在
func (filter *MemoryBloomFilter) HasString(data string) bool {
	return filter.Has([]byte(data))
}

// Close 关闭bloom filter
func (filter *MemoryBloomFilter) Close() {
	filter.bs = nil
}


func NewFileBloomFilter(target string, n uint, k uint) *FileBloomFilter {
	memoryFilter := NewMemoryBloomFilter(n, k)
	filter := &FileBloomFilter{
		memoryFilter, target,
	}
	filter.reStore()

	return filter
}

func (filter *FileBloomFilter) Close() {
	filter.store()
	filter.bs = nil
}

func (filter *FileBloomFilter) store() {
	f, err := os.Create(filter.target)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Open file"))
	}
	defer f.Close()

	gzipWriter := gzip.NewWriter(f)
	defer gzipWriter.Close()

	encoder := gob.NewEncoder(gzipWriter)
	err = encoder.Encode(filter.bs)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "gzip"))
	}
}

func (filter *FileBloomFilter) reStore() {
	f, err := os.Open(filter.target)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("%+v", errors.Wrap(err, "Open file"))
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "Ungzip"))
	}

	decoder := gob.NewDecoder(gzipReader)
	err = decoder.Decode(&filter.bs)
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, "gob decode"))
	}
}

func NewRedisBloomFilter(cli redis.Conn, n, k uint) *RedisBloomFilter {
	filter := &RedisBloomFilter{
		cli: cli,
		n:   n,
		k:   k,
	}
	length, _ := redis.Int64(cli.Do("LLEN", filter.redisKey()))
	if uint(length) != n {
		bs := make([]interface{}, n)
		pushArgs := []interface{}{filter.redisKey()}
		pushArgs = append(pushArgs, bs...)
		cli.Do("DEL", filter.redisKey())
		cli.Do("LPUSH", pushArgs...)
	}

	return filter
}

func (filter *RedisBloomFilter) Put(data []byte) {
	for i := uint(0); i < filter.k; i++ {
		_, err := filter.cli.Do("LSET", filter.redisKey(), HashData(data, i)%filter.n, "1")
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LSET"))
		}
	}
}

func (filter *RedisBloomFilter) PutString(data string) {
	filter.Put([]byte(data))
}

func (filter *RedisBloomFilter) Has(data []byte) bool {
	for i := uint(0); i < filter.k; i++ {
		index := HashData(data, i) % filter.n
		value, err := redis.String(filter.cli.Do("LINDEX", filter.redisKey(), index))
		if err != nil {
			log.Fatalf("%+v", errors.Wrap(err, "LINDEX"))
		}
		if value != "1" {
			return false
		}
	}

	return true
}

func (filter *RedisBloomFilter) HasString(data string) bool {
	return filter.Has([]byte(data))
}

// Close 只将cli设置为nil, 关闭redis连接的操作放在调用处
func (filter *RedisBloomFilter) Close() {
	filter.cli = nil
}

// redisKey 根据filter的n和k来生成一个独立的redis key
func (filter *RedisBloomFilter) redisKey() string {
	return fmt.Sprintf("_bloomfilter:n%d:k%d", filter.n, filter.k)
}
