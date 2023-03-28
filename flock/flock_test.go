package flock

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// 不要一次执行多次测试（>5次），否则可能会出现“打开的文件太多”错误
func TestAcquireFileLock(t *testing.T) {
	testFn := func(readOnly bool, times int, actual int) {
		// 准备数据
		path, err := filepath.Abs(filepath.Join("/tmp", "flock-test"))
		assert.Nil(t, err)
		err = os.MkdirAll(path, os.ModePerm)
		assert.Nil(t, err)

		var count uint32
		var flock *FileLockGuard

		defer func() {
			if flock != nil {
				_ = flock.Release()
			}
			if err = os.RemoveAll(path); err != nil {
				t.Error(err)
			}
		}()

		wg := &sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				defer wg.Done()
				lock, err := AcquireFileLock(filepath.Join(path, "FLOCK"), readOnly)
				if err != nil {
					atomic.AddUint32(&count, 1)
				} else {
					flock = lock
				}
				if readOnly && times > 1 && lock != nil {
					_ = lock.Release()
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, count, uint32(actual))
	}
	// 独用的锁
	t.Run("exclusive-1", func(t *testing.T) {
		testFn(false, 1, 0)
	})

	t.Run("exclusive-2", func(t *testing.T) {
		testFn(false, 10, 9)
	})

	t.Run("exclusive-3", func(t *testing.T) {
		testFn(false, 15, 14)
	})
	// 共享的锁
	t.Run("shared-1", func(t *testing.T) {
		testFn(true, 1, 0)
	})

	t.Run("shared-2", func(t *testing.T) {
		testFn(true, 15, 0)
	})
}

func TestAcquireFileLock_NotExist(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "flock", "test"))
	assert.Nil(t, err)
	_, err = AcquireFileLock(path+string(os.PathSeparator)+"FLOCK", false)
	assert.NotNil(t, err)
}

func TestFileLockGuard_Release(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "flock-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	lock, err := AcquireFileLock(filepath.Join(path, "FLOCK"), false)
	assert.Nil(t, err)
	err = lock.Release()
	assert.Nil(t, err)
}
