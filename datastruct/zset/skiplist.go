package zset

import "math/rand"

// 跳表实现

const (
	// 跳表索引链表的最高等级
	maxLevel    = 32
	probability = 0.25
)

type (
	// 索引等级链表
	sklLevel struct {
		forward *sklNode // 前进指针，指向下一个节点
		span    uint64   // 到下一个节点的距离
	}

	// 跳表节点
	sklNode struct {
		member   string
		score    float64
		backward *sklNode    // 后退指针，可以反向查找
		level    []*sklLevel // 多个索引等级链表
	}

	// 跳表
	skipList struct {
		head   *sklNode // 头节点
		tail   *sklNode // 尾节点
		length int64    // 长度
		level  int16    // 最大等级
	}
)

// 创建跳表节点
func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		member: member,
		score:  score,
		level:  make([]*sklLevel, level),
	}
	// 初始化节点的level
	for i := range node.level {
		node.level[i] = new(sklLevel)
	}
	return node
}

// 创建跳表
func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

// 随机生成新节点的层级高度
func randomLevel() int16 {
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return maxLevel
	}
	return maxLevel
}

// 插入跳表节点
func (skl *skipList) sklInsert(score float64, member string) *sklNode {

	// updates: updates保存了每一层插入位置的前向结点地址，这些结点的span值都需要改动
	// rank: rank保存着每一层插入位置前向结点的排名，是为span准备的
	updates := make([]*sklNode, maxLevel)
	rank := make([]uint64, maxLevel)

	p := skl.head
	// 遍历索引等级链表
	for i := skl.level - 1; i >= 0; i-- {
		// 层级降低，则该层的rank起始值就是上层rank终止值
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 遍历当前层等级链表
		if p.level[i] != nil {
			// 循环直到 当前结点p的下一结点的score大于等于插入的score，也就是新节点是插到p后面的
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
				// 在前进过程中该层的排名rank累加
				rank[i] += p.level[i].span
				// 移动到下一个节点
				p = p.level[i].forward
			}
		}
		// 新节点插入到p结点后面
		updates[i] = p
	}

	// 随机生成新节点的层级高度
	level := randomLevel()
	if level > skl.level {
		for i := skl.level; i < level; i++ {
			/*
			   如果新节点的level比其他结点的level都要高，也就是说新节点是最高的，则新加一层，
			   那么新节点的前向结点就是skipList的头结点，
			   而此时这一层的span应该是从头到尾的，也就是整个链表的长度
			*/
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	// 创建插入的节点
	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 插入节点
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p
		// 新增结点的每一层都要更新span值
		// 计算新节点跨越的节点数量   找到的那个节点的跨度   //0层与i层所对应update的RANK
		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		// 指向新结点每一层的结点都需要更新span值
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	// 如果新节点的level不是最高，那么需要更新那些比它高的结点的span值
	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}
	// 更新backward的值
	if updates[0] == skl.head {
		p.backward = nil
	} else {
		p.backward = updates[0]
	}

	if p.level[0].forward != nil {
		// 下一结点的后退指针
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}
	// 长度加1
	skl.length++
	return p
}

// 更新所有和被删除节点 p 有关的节点的指针，解除它们之间的关系
func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	// 更新所有节点的span值
	for i := int16(0); i < skl.level; i++ {
		//  update[]和p在同层级
		if updates[i].level[i].forward == p {
			// 加上要删除的元素的span 再减去删除的元素本身
			updates[i].level[i].span += p.level[i].span - 1
			// 更新forward
			updates[i].level[i].forward = p.level[i].forward
		} else {
			// update和p不在同层，也就是说update的level比p的level要高，此时直接更新update的span值
			updates[i].level[i].span--
		}
	}

	// 修改L0的backward指针
	if p.level[0].forward != nil {
		// 被删除结点不是尾结点，则后向结点的前向指针应该是删除结点的前向结点
		p.level[0].forward.backward = p.backward
	} else {
		skl.tail = p.backward
	}
	//  当最高层的最后一个节点被删除时更新最大层数
	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	// 长度减1
	skl.length--
}

// 删除跳表节点
func (skl *skipList) sklDelete(score float64, member string) {
	// updates记录了每一层被删除结点的前向结点
	updates := make([]*sklNode, maxLevel)
	p := skl.head
	// 遍历索引等级链表
	for i := skl.level - 1; i >= 0; i-- {
		// 在这一层上一直往前走，直到出现score大于等于要删除的结点
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		// 记录沿途节点，即该层删除结点p的前向结点
		updates[i] = p
	}

	p = p.level[0].forward
	// 第0层找到要删除的节点
	if p != nil && score == p.score && member == p.member {
		// 更新向前结点的span值
		skl.sklDeleteNode(p, updates)
		return
	}
}

// 查找节点的排名
func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head
	// 遍历索引等级链表
	for i := skl.level - 1; i >= 0; i-- {
		// 该层结点不为空，且score都小于查找score，或者是在score相同的情况下member小于查找member
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {
			// 排名就是所有已跳过的span总和
			rank += p.level[i].span
			p = p.level[i].forward
		}
		// 找到了就直接返回
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

// 通过rank获取节点
func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head
	// 遍历索引等级链表
	for i := skl.level - 1; i >= 0; i-- {
		// 该层节点不空，且已跳过的span总和小于等于rank，
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			// traversed所有已跳过的span总和
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}
	return nil
}
