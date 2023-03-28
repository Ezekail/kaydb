package benchmark

import (
	"fmt"
	"github.com/Ezekail/kaydb"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var kayDB *kaydb.KayDB

func init() {
	path := filepath.Join("/tmp", "rosedb_bench")
	opts := kaydb.DefaultOptions(path)
	var err error
	kayDB, err = kaydb.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("open rosedb err: %v", err))
	}
	initDataForGet()
}

func initDataForGet() {
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := kayDB.Set(getKey(i), getValue128B())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := kayDB.Set(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkRoseDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := kayDB.Get(getKey(i))
		assert.Nil(b, err)
	}
}

func BenchmarkRoseDB_LPush(b *testing.B) {
	keys := [][]byte{
		[]byte("my_list-1"),
		[]byte("my_list-2"),
		[]byte("my_list-3"),
		[]byte("my_list-4"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := kayDB.LPush(keys[k], getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkRoseDB_ZAdd(b *testing.B) {
	keys := [][]byte{
		[]byte("my_zset-1"),
		[]byte("my_zset-2"),
		[]byte("my_zset-3"),
		[]byte("my_zset-4"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := kayDB.ZAdd(keys[k], float64(i+100), getValue128B())
		assert.Nil(b, err)
	}
}
