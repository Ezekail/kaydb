package mmap

import (
	"github.com/Ezekail/kaydb/logger"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestMmap(t *testing.T) {
	// 创建临时文件夹
	dir, err := ioutil.TempDir("", "kaydb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")

	// 打开文件
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	// 记得关闭文件，删除文件夹
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(dir, t)
		}
	}()
	type args struct {
		fd       *os.File
		writable bool
		size     int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"normal-size", args{fd: fd, writable: true, size: 100}, false},
		{"big-size", args{fd: fd, writable: true, size: 128 << 20}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Mmap(tt.args.fd, tt.args.writable, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mmap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if int64(len(got)) != tt.args.size {
				t.Errorf("Mmap() want buf size = %d, actual = %d", tt.args.size, len(got))
			}
		})
	}
}

func TestMunmap(t *testing.T) {
	// 先创建临时文件夹
	dir, err := ioutil.TempDir("", "kaydb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")
	// 创建文件
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(dir, t)
		}
	}()

	buf, err := Mmap(fd, true, 100)
	assert.Nil(t, err)
	err = Munmap(buf)
	assert.Nil(t, err)
}

func TestMsync(t *testing.T) {
	// 先创建临时文件夹
	dir, err := ioutil.TempDir("", "kaydb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")
	// 创建文件
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(dir, t)
		}
	}()

	buf, err := Mmap(fd, true, 100)
	assert.Nil(t, err)
	err = Msync(buf)
	assert.Nil(t, err)
}

func destroyDir(dir string, t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		logger.Warnf("remove dir err:%v", err)
	}
}
