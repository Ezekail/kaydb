package art

import goart "github.com/plar/go-adaptive-radix-tree"

// AdaptiveRadixTree 自适应基数树
type AdaptiveRadixTree struct {
	tree goart.Tree
}

// NewART 创建一棵AdaptiveRadixTree
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

// Put 在树中插入新key。如果key已经在树中，则返回oldValue，true，否则返回nil和false
func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldValue interface{}, updates bool) {
	return art.tree.Insert(key, value)
}

// Get 搜索返回特定key的值
func (art *AdaptiveRadixTree) Get(key []byte) interface{} {
	// 如果key存在，则返回值和true，否则返回nil和false
	value, _ := art.tree.Search(key)
	return value
}

// Delete 从树中删除一个key，并返回key的值和true。如果key不存在，则不执行任何操作，返回nil，false
func (art *AdaptiveRadixTree) Delete(key []byte) (value interface{}, updated bool) {
	return art.tree.Delete(key)
}

// Iterator 迭代器按key顺序遍历节点。
func (art *AdaptiveRadixTree) Iterator() goart.Iterator {
	// 默认情况下，返回一个迭代器，用于对叶节点进行预排序遍历。
	// 将遍历XXX作为返回迭代器的选项，用于对所有NodeXXX类型进行预排序遍历
	return art.tree.Iterator()
}

// PrefixScan 前缀扫描
func (art *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	// 树遍历的回调函数,如果回调函数返回false，则终止迭代。
	cb := func(node goart.Node) (cont bool) {
		if node.Kind() != goart.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}
	if len(prefix) == 0 {
		art.tree.ForEach(cb)
	} else {
		art.tree.ForEachPrefix(prefix, cb)
	}
	return
}

// Size 返回Size的大小
func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}
