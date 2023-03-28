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
		fmt.Printf("open db err:%v\n", err)
		return
	}

	err = db.SAdd([]byte("fruits"), []byte("watermelon"), []byte("grape"), []byte("orange"), []byte("apple"))
	if err != nil {
		fmt.Printf("SAdd err : %v\n", err)
	}

	err = db.SAdd([]byte("fav-fruits"), []byte("orange"), []byte("melon"), []byte("strawberry"))
	if err != nil {
		fmt.Printf("SAdd err : %v\n", err)
	}

	diff, err := db.SDiff([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SDiff err:%v\n", err)
	}
	fmt.Println("SDiff set:")
	for _, val := range diff {
		fmt.Printf("%v\n", string(val))
	}

	union, err := db.SUnion([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SUnion err:%v\n", err)
	}
	fmt.Println("SUnion set:")
	for _, val := range union {
		fmt.Printf("%v\n", string(val))
	}
}
