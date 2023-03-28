package zset

import "math"

// 对zest的内存索引操作

type (
	// SortedSet 有序集合结构体
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	// SortedSetNode 有序集合节点
	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}
)

// NewSortedSet 创建一个新的有序集合
func NewSortedSet() *SortedSet {
	return &SortedSet{
		record: make(map[string]*SortedSetNode),
	}
}

// ZAdd 将具有指定分数的指定成员添加到存储在key中的有序集合
// member存在则修改score
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	// key 不存在
	if !z.exist(key) {
		// 创建新的有序集合节点
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		z.record[key] = node
	}

	// 获取跳表节点，判断是否存在
	item := z.record[key]
	v, exist := item.dict[member]
	var node *sklNode
	if exist {
		// 若存在，则判断分值是否相等，不相等则删除旧值，插入新值
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	} else {
		// 若不存在，则创建并插入新节点
		node = item.skl.sklInsert(score, member)
	}

	if node != nil {
		item.dict[member] = node
	}
}

// ZScore 返回key处有序集合中member的score
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	// key不存在，直接返回
	if !z.exist(key) {
		return
	}
	// 获取跳表节点，不存在则直接返回
	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}
	return true, node.score
}

// ZCard 返回存储在key中的有序集合的基数（元素数）,即member的总数
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

// ZRank 返回存储在key中的有序集合中成员的排名，分数从低到高排序
// 排名（或索引）从0开始，这意味着得分最低的成员的排名为0
func (z *SortedSet) ZRank(key, member string) int64 {
	// key不存在，返回-1
	if !z.exist(key) {
		return -1
	}
	// 获取跳表节点，不存在返回-1
	node, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}
	// 获取排名rank
	rank := z.record[key].skl.sklGetRank(node.score, member)
	rank--
	return rank
}

// ZRevRank 返回存储在key中的已排序集合中成员的排名，分数从高到低排序
// 排名（或索引）从0开始，这意味着得分最高的成员的排名为0
func (z *SortedSet) ZRevRank(key, member string) int64 {
	// key 不存在，返回-1
	if !z.exist(key) {
		return -1
	}
	// 获取跳表节点，不存在返回-1
	node, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}
	// 获取排名rank
	rank := z.record[key].skl.sklGetRank(node.score, member)

	return z.record[key].skl.length - rank
}

// ZIncrBy 将存储在key处的有序集合中成员的分数递增
// 如果成员在有序集合中不存在，则将以增量作为其分数（就像其上一个分数是0.0一样）添加该成员
// 如果key不存在，将创建一个新的有序集合，其中指定的成员作为其唯一成员
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	// member存在，则增加score
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}
	// 不存在增加，存在则修改
	z.ZAdd(key, increment, member)
	return increment
}

// ZRange 返回存储在<key>中的有序集中指定的元素范围
func (z *SortedSet) ZRange(key string, start, end int) []interface{} {
	// key 不存在直接返回
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), false, false)
}

// ZRangeWithScores 返回存储在<key>中的有序集中指定的元素范围
func (z *SortedSet) ZRangeWithScores(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), false, true)
}

// ZRevRange 返回存储在<key>处的有序集中指定的元素范围
// 元素被认为是从最高到最低的顺序
// 对于分数相等的元素，使用降序词典顺序
func (z *SortedSet) ZRevRange(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), true, false)
}

// ZRevRangeWithScores 返回存储在<key>处的有序集中指定的元素范围
// 元素被认为是从最高到最低的顺序
// 对于分数相等的元素，使用降序词典顺序
func (z *SortedSet) ZRevRangeWithScores(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}
	return z.findRange(key, int64(start), int64(end), true, true)
}

// ZRem 从存储在key中的有序集合中删除指定的成员，忽略不存在的成员
// 当key存在且不包含有序集合时，将返回错误
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}
	// 获取跳表节点
	node, exist := z.record[key].dict[member]
	if exist {
		// 删除跳表节点与哈希表的key
		z.record[key].skl.sklDelete(node.score, member)
		delete(z.record[key].dict, member)
		return true
	}
	return false
}

// ZGetByRank 按rank获取key的成员，rank从最低到最高排序
// 最低等级为0，依此类推
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return nil
	}
	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank 按rank获取key的成员，级别从最高到最低排序。
// 最高等级为0，依此类推
func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return nil
	}
	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

// ZScoreRange 返回有序集合中的所有元素，其score在min和max之间（包括score等于min或max的元素）
// 元素被认为是从低到高分排序的
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	// key不存在，或者输入越界，直接返回
	if !z.exist(key) || min > max {
		return nil
	}

	// 获取最低分minScore
	skl := z.record[key].skl
	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	// 获取最高分maxScore
	maxScore := skl.tail.score
	if max > maxScore {
		max = maxScore
	}

	// 遍历找到 >= min的节点p
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}
	// 将min 到 max之间的节点追加到val
	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}
		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return val
}

// ZRevScoreRange 返回有序集合key中score在max和min之间的所有元素（包括score等于max或min的元素）
// 与有序集合的默认排序相反，对于该命令，元素被认为是从高分到低分排序的
func (z *SortedSet) ZRevScoreRange(key string, min, max float64) (val []interface{}) {
	// key不存在，或者输入越界，直接返回
	if !z.exist(key) || max < min {
		return nil
	}
	// 获取最低分minScore
	skl := z.record[key].skl
	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	// 获取最高分maxScore
	maxScore := skl.tail.score
	if max > maxScore {
		max = maxScore
	}

	// 找到 > max 的节点p
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}
	// 将max 到 min之间的节点追加到val
	for p != nil {
		if p.score < min {
			break
		}
		val = append(val, p.member, p.score)
		p = p.backward
	}
	return val
}

// ZClear 清除ZSet中的key
func (z *SortedSet) ZClear(key string) {
	// 存在则删除dict中的key
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

// ZKeyExists 检查ZSet中是否存在key
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// 判断key是否存在
func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

// 通过rank，获取member, score
// reverse为true，rank从尾开始
func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {
	skl := z.record[key].skl
	// rank 越界
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}
	// reverse为true从尾到头
	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}
	// 通过rank获取节点
	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}
	// 获取dict中的跳表节点
	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}
	return node.member, node.score
}

// reverse为true则从尾到头遍历查找，否则从头到尾遍历查找
// withScores为true返回member和score，否则返回member
func (z *SortedSet) findRange(key string, start, end int64, reverse, withScores bool) (val []interface{}) {
	// 获取跳表
	skl := z.record[key].skl
	length := skl.length

	// start为负数，不超长度，表示从后向前数
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += length
	}
	// 越界判断
	if start > end || start >= length {
		return
	}
	if end >= length {
		end = length - 1
	}
	span := (end - start) + 1

	// reverse为true 从尾开始查找
	var node *sklNode
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	// withScores为true则返回member和score
	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
	return
}
