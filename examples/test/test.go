package main

import (
	"fmt"
	"path/filepath"
	"sort"
)

var keys = map[string]string{
	"hello":       "world",
	"foo":         "bar",
	"baz":         "qux",
	"raboof":      "oofbar",
	"quux":        "quuz",
	"corge":       "grault",
	"garply":      "waldo",
	"fred":        "plugh",
	"xyzzy":       "thud",
	"test_key_1":  "test_value_1",
	"test_key_2":  "test_value_2",
	"test_key_3":  "test_value_3",
	"test_key_4":  "test_value_4",
	"test_key_5":  "test_value_5",
	"test_key_6":  "test_value_6",
	"test_key_7":  "test_value_7",
	"test_key_8":  "test_value_8",
	"test_key_9":  "test_value_9",
	"test_key_10": "test_value_10",
}

func scan(cursor string, pattern string, count int) (string, []string) {
	// 扫描游标之后的键
	var matchingKeys []string
	for k := range keys {
		if ok, _ := filepath.Match(pattern, k); ok && k > cursor {
			matchingKeys = append(matchingKeys, k)
		}
	}

	// 对符合模式的键进行排序
	sort.Strings(matchingKeys)

	// 限制扫描结果的数量
	var scanResults []string
	for i := 0; i < count && i < len(matchingKeys); i++ {
		scanResults = append(scanResults, matchingKeys[i])
	}

	// 设置新的游标
	newCursor := ""
	if len(matchingKeys) > count && len(scanResults) > 0 {
		newCursor = scanResults[len(scanResults)-1]
	}

	return newCursor, scanResults
}

func main() {
	var cursor string
	var keys []string

	// 分页扫描符合 "*key*" 模式的键，每页 3 个键

	cursor, keys = scan(cursor, "*key*", 3)
	fmt.Println(keys)

	// 如果返回的游标值为空字符串，表示扫描结束，可以退出循环

}
