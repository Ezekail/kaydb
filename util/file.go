package util

import "os"

// PathExist 检查目录或文件是否存在
func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
