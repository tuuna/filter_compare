package main

import (
	"fmt"
	"testing"

	"os"

	"github.com/gomodule/redigo/redis"
)

func RandTest(t *testing.T, filter BloomFilter, n int) {
	for i := 0; i < n; i++ {
		filter.PutString(fmt.Sprintf("r%d", i))
	}

	var missNumbers int

	for i := 0; i < n; i++ {
		existsRecord := fmt.Sprintf("r%d", i)
		notExistsRecord := fmt.Sprintf("rr%d", i)
		if !filter.HasString(existsRecord) {
			missNumbers++
		}

		if filter.HasString(notExistsRecord) {
			missNumbers++
		}
	}
	hitRate := float64(n - missNumbers) / float64(n)
	fmt.Printf("hit rate: %f\n", hitRate)

	if hitRate < 0.9 {
		t.Fatalf("Oh, fuck. hit rate is %f, too low", hitRate)
	}
}

func TestMemoryBloomFilter(t *testing.T) {
	var filter BloomFilter = NewMemoryBloomFilter(64<<20, 5)
	RandTest(t, filter, 50000)
}

func TestFileBloomFilter(t *testing.T) {
	target := "bloom.tmp"
	defer os.Remove(target)
	var filter BloomFilter = NewFileBloomFilter(target, 64<<20, 5)
	RandTest(t, filter, 50000)
}

func TestRedisBloomFilter(t *testing.T) {
	cli, err := redis.DialURL("redis://127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	var filter BloomFilter = NewRedisBloomFilter(cli, 2000, 5)
	RandTest(t, filter, 50)
}