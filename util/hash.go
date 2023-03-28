package util

import "unsafe"

//go:linkname runtimeMemhash runtime.memhash
//go:noescape

func runtimeMemhash(p unsafe.Pointer, seed, s uintptr) uintptr

// 取消内联优化 ⬆

// MemHash 是go map使用的哈希函数，它利用可用的硬件指令（如果aes指令可用，则表现为aeshash）
// 注意：每个进程的哈希种子都会更改。因此，这不能用作持久散列
func MemHash(buf []byte) uint64 {
	return rthash(buf, 923)
}

func rthash(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return seed
	}

	// 运行时哈希器仅在uintptr上工作。对于64位架构，我们直接使用哈希器
	// 否则，我们在低位和高位32位上使用两个并行散列器
	if unsafe.Sizeof(uintptr(0)) == 8 {
		return uint64(runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))
	}
	lo := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b)))
	hi := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed>>32), uintptr(len(b)))
	return uint64(hi)<<32 | uint64(lo)
}
