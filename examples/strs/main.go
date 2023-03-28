package main

import (
	"fmt"
	"github.com/Ezekail/kaydb"
	"path/filepath"
	"time"
)

func main() {
	// str 的使用示例
	path := filepath.Join("/tmp", "kaydb")
	opts := kaydb.DefaultOptions(path)
	db, err := kaydb.Open(opts)
	if err != nil {
		fmt.Printf("open kaydb err:%v\n", err)
		return
	}
	err = db.Set([]byte("scan2"), []byte("1"))
	if err != nil {
		fmt.Printf("set data err:%v\n", err)
		return
	}
	valInt64, err := db.Incr([]byte("scan2"))
	if err != nil {
		fmt.Printf("incr data err:%v\n", err)
		return
	}
	fmt.Println(valInt64)

	err = db.Set([]byte("name"), []byte("KayDB"))
	if err != nil {
		fmt.Printf("write data err:%v\n", err)
		return
	}

	v, err := db.Get([]byte("name"))
	if err != nil {
		fmt.Printf("read data err:%v\n", err)
		return
	}
	fmt.Println("val = ", string(v))

	err = db.SetEX([]byte("type"), []byte("KayDB-str"), time.Second*5)
	if err != nil {
		fmt.Printf("write data with ex err:%v\n", err)
		return
	}
	fmt.Println("SetEX key-value pair added.")

	err = db.Delete([]byte("name"))
	if err != nil {
		fmt.Printf("delete data err:%v\n", err)
		return
	}
	fmt.Println("Delete key success.")

	err = db.SetNX([]byte("SetNX"), []byte("SetNX-value"))
	if err != nil {
		fmt.Printf("write data nx err:%v\n", err)
		return
	}
	fmt.Println("SetNX key-value pair added.")

	v, err = db.Get([]byte("SetNX"))
	if err != nil {
		fmt.Printf("read SetNX data err:%v\n", err)
		return
	}
	fmt.Printf("SetNX-type = %s\n", v)

	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"), []byte("value-2"))
	if err != nil {
		fmt.Printf("MSet err:%v\n", err)
		return
	}
	fmt.Println("Multiple key-value pair added.")

	// Missing value.
	err = db.MSet([]byte("key-1"), []byte("value-1"), []byte("key-2"))
	if err != nil {
		fmt.Printf("A example of missing value,mset error: %v\n", err)
	}

	err = db.MSetNX([]byte("key-11"), []byte("value-11"), []byte("key-22"), []byte("value-22"))
	if err != nil {
		fmt.Printf("msetnx error: %v\n", err)
	}
	val, _ := db.Get([]byte("key-11"))
	fmt.Printf("key-11: %v\n", string(val))

	// mget
	keys := [][]byte{
		[]byte("key-1"),
		[]byte("not exist"),
		[]byte("key-11"),
	}
	values, err := db.MGet(keys)
	if err != nil {
		fmt.Printf("mget err : %v\n", err)
	} else {
		for _, value := range values {
			fmt.Printf("mget values : %v - ", string(value))
		}
	}

	// example of append
	err = db.Delete([]byte("append"))
	if err != nil {
		fmt.Printf("delete data err:%v\n", err)
		return
	}

	_, err = db.GetDel([]byte("not-exist"))
	if err != nil {
		fmt.Printf("getdel data err:%v\n", err)
	}
	gdVal, err := db.GetDel([]byte("key-22"))
	if err != nil {
		fmt.Printf("getdel data err:%v\n", err)
	} else {
		fmt.Println("getdel val :", string(gdVal))
	}

	err = db.Append([]byte("append"), []byte("Kay"))
	if err != nil {
		fmt.Printf("append data err: %v\n", err)
		return
	}

	err = db.Append([]byte("append"), []byte("DB"))
	if err != nil {
		fmt.Printf("append data err:%v\n", err)
		return
	}

	v, err = db.Get([]byte("append"))
	if err != nil {
		fmt.Printf("read data err:%v\n", err)
		return
	}
	fmt.Printf("append data = %s\n ", string(v))

	strLen := db.StrLen([]byte("key-1"))
	fmt.Printf("strlen : %v\n", strLen)

	_ = db.Set([]byte("int"), []byte("12"))
	valInt, err := db.Decr([]byte("int"))
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Printf("new value after Decr():%v\n", valInt)

	valInt, err = db.DecrBy([]byte("int"), 5)
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Printf("new value after DecrBy(5):%v \n", valInt)
}
