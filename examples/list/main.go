package main

import (
	"fmt"
	"github.com/Ezekail/kaydb"
	"path/filepath"
)

func main() {
	path := filepath.Join("/tmp", "kaydb")
	opts := kaydb.DefaultOptions(path)
	db, err := kaydb.Open(opts)
	if err != nil {
		fmt.Printf("open kaydb err: %v\n", err)
		return
	}

	// dataStruct: Kay,Ming, Jame, Tom
	err = db.LPush([]byte("students"), []byte("Tom"), []byte("Jane"), []byte("Ming"))
	if err != nil {
		fmt.Printf("LPush write data err:%v\n", err)
		return
	}

	err = db.LPushX([]byte("not-exist"), []byte("Tom"))
	fmt.Println(err) // ErrKeyNotFound
	err = db.LPushX([]byte("students"), []byte("Kay"))
	if err != nil {
		fmt.Printf("LPushX write data err:%v\n", err)
		return
	}

	values, err := db.LRange([]byte("students"), 0, 4)
	if err != nil {
		fmt.Printf("range data err:%v\n", err)
		return
	}
	for _, value := range values {
		fmt.Printf("LPush data after: %v ", string(value))
	}

	// dataStruct: Kay,Ming, Jame, Tom, Jack, Wei
	err = db.RPush([]byte("students"), []byte("Jack"), []byte("Wei"))
	if err != nil {
		fmt.Printf("RPush write data err:%v\n", err)
		return
	}

	err = db.RPushX([]byte("not-exist"), []byte("Jack"))
	fmt.Println(err) // ErrKeyNotFound
	err = db.RPushX([]byte("students"), []byte("DB"))
	if err != nil {
		fmt.Printf("RPushX write data err:%v\n", err)
		return
	}

	values, err = db.LRange([]byte("students"), 0, 5)
	if err != nil {
		fmt.Printf("range data err:%v\n", err)
		return
	}
	for _, value := range values {
		fmt.Printf("RPush data after: %v ", string(value))
	}

	stuLens := db.LLen([]byte("students"))
	fmt.Println(stuLens)

	// out: Ming
	// dataStruct: Jame, Tom, Jack, Wei
	lPopStu, err := db.LPop([]byte("students"))
	if err != nil {
		fmt.Printf("lpop data err: %v", err)
		return
	}
	fmt.Println(string(lPopStu))

	// out: Wei
	// dataStruct: Jame, Tom, Jack
	rPopStu, err := db.RPop([]byte("students"))
	if err != nil {
		fmt.Printf("rpop data err: %v", err)
		return
	}
	fmt.Println(string(rPopStu))
}
