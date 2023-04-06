package main

import (
	"fmt"
	"github.com/Ezekail/kaydb"
	"path/filepath"
)

func main() {
	path1 := filepath.Join("/tmp", "kaydb")
	path := filepath.Join(path1, fmt.Sprintf("kaydb-%04d", 0))
	opts := kaydb.DefaultOptions(path)
	db, err := kaydb.Open(opts)
	if err != nil {
		fmt.Printf("open kaydb err:%v\n", err)
		return
	}
	err = db.LPush([]byte("Lkey2"), []byte("111"))
	if err != nil {
		fmt.Printf("LPush err:%v\n", err)
		return
	}
	err = db.LPush([]byte("Lkey3"), []byte("111"))
	if err != nil {
		fmt.Printf("LPush err:%v\n", err)
		return
	}
	keys := db.LScan()
	lRange := make([][]byte, 0)
	for _, key := range keys {
		lRange, err = db.LRange([]byte(key), 0, 500)
		if err != nil {
			fmt.Printf("LRange err:%v\n", err)
			return
		}
		for i, bytes := range lRange {
			fmt.Printf("key:%v,index:%v,val:%v\n", key, i, string(bytes))
		}
	}

	//vals, err := db.StrScan([]byte(""), "*", 500)
	//if err != nil {
	//	fmt.Printf("strScan failed,err:%v\n", err)
	//	return
	//}
	//for i := 0; i < len(vals); i += 2 {
	//	fmt.Printf("key:%v,val:%v\n", string(vals[i]), string(vals[i+1]))
	//}
}
