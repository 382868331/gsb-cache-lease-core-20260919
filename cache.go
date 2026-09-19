// Package leasecache 实现一个字符串键值的内存缓存协调库。
//
// 核心语义（详见 TASK.md）：
//   - 时间（tick）由调用方传入，是非负 int64；所有成功的修改类操作与 Acquire
//     在线性化顺序上维护一个全局高水位 now，小于高水位拒绝（时间回退），相等允许。
//   - Acquire 返回三种结果：Hit（命中缓存）、Leader（成为该键当前分代唯一的
//     在途租约持有者）、Pending（已有同键同代租约，返回其 leaseID）。
//   - Complete 携带租约回填值与 ttl；失效分代的旧租约回填一律返回 stale，
//     不会污染新分代。
//   - Invalidate/Clear 使对应缓存项与租约失效；租约只由 Complete/Fail/
//     Invalidate/Clear 结束，不会因为时间流逝自动消失。
//   - 容量只统计缓存项：写入前先剔除所有已过期项，再按 LRU 序号驱逐。
//   - 任何错误（stale、时间回退、超在途上限、非法 ttl 等）都不改变数据、
//     LRU、租约或时钟。
//
// 全部 API 持同一把互斥锁，因而可线性化，无需无锁算法。
package leasecache

import (
	"errors"
	"math"
	"sync"
)

// 哨兵错误。调用方使用 errors.Is 判断。
var (
	// ErrInvalidConfig 表示构造参数非法（容量或在途上限不是正整数）。
	ErrInvalidConfig = errors.New("leasecache: capacity and maxInFlight must be positive")
	// ErrNegativeTick 表示调用方传入了负 tick。
	ErrNegativeTick = errors.New("leasecache: tick must be non-negative")
	// ErrRollback 表示 tick 小于当前全局高水位（时间回退）。
	ErrRollback = errors.New("leasecache: tick predates the high-water mark")
	// ErrStaleLease 表示租约未知或已失效（已 Complete/Fail，或其分代被
	// Invalidate/Clear 淘汰）；旧租约的回填不会产生任何效果。
	ErrStaleLease = errors.New("leasecache: lease is unknown or stale")
	// ErrInvalidTTL 表示 ttl<=0 或 now+ttl 溢出 int64。
	ErrInvalidTTL = errors.New("leasecache: ttl must be positive and now+ttl must not overflow")
	// ErrInFlightLimit 表示在途租约已达上限，新 Leader 被拒绝。
	ErrInFlightLimit = errors.New("leasecache: in-flight lease limit reached")
	// ErrLeaseIDsExhausted 表示租约 ID 计数耗尽（ID 永不复用）。
	ErrLeaseIDsExhausted = errors.New("leasecache: lease id space exhausted")
)

// Kind 是 Acquire 结果的三种类型。
type Kind uint8

const (
	// KindHit 命中缓存，Result.Value 为缓存值。
	KindHit Kind = iota + 1
	// KindLeader 调用方成为该键当前分代唯一的加载租约持有者。
	KindLeader
	// KindPending 该键当前分代已存在在途租约，Result.LeaseID 可查。
	KindPending
)

// Lease 是 Leader 持有的不透明租约句柄。租约 ID 全局不复用。
type Lease struct {
	id  uint64
	key string
	gen uint64
}

// ID 返回租约的全局唯一 ID（从 1 开始，不复用）。
func (l *Lease) ID() uint64 { return l.id }

// Key 返回租约对应的键。
func (l *Lease) Key() string { return l.key }

// Generation 返回租约所属的键分代。
func (l *Lease) Generation() uint64 { return l.gen }

// Result 是 Acquire 的返回值：
//   - KindHit     ：Value 有效
//   - KindLeader  ：Lease 与 LeaseID 有效
//   - KindPending ：LeaseID 有效
type Result struct {
	Kind    Kind
	Value   string
	Lease   *Lease
	LeaseID uint64
}

// Stats 是不推进时钟的只读快照。
type Stats struct {
	// Items 是当前缓存项数量（只包含物理存在的项）。
	Items int
	// InFlight 是当前在途租约数量。
	InFlight int
	// Keys 是库内部跟踪的键状态数量（含无缓存项、无租约的键）。
	Keys int
	// HighWater 是全局 tick 高水位。
	HighWater int64
	// Epoch 在每次 Clear 成功后递增。
	Epoch uint64
}

type entry struct {
	value     string
	expiresAt int64
	lruSeq    uint64
}

type keyState struct {
	gen     uint64 // 该键当前分代；Invalidate/Clear 单调递增
	leaseID uint64 // 当前在途租约 ID，0 表示无
	hasItem bool
	item    entry
}

// leaseRec 是租约 ID -> (键, 分代) 的权威索引。
// 租约结束（Complete/Fail/Invalidate/Clear）即从索引删除，
// 因此旧句柄再次出现会被判定为 stale，且 ID 永不复用。
type leaseRec struct {
	key string
	gen uint64
}

// Cache 是并发安全的缓存租约协调器。零值不可用，请用 New 构造。
type Cache struct {
	mu sync.Mutex

	capacity    int
	maxInFlight int

	states   map[string]*keyState
	leases   map[uint64]*leaseRec
	inFlight int
	nItems   int // == 遍历 states 中 hasItem 的计数，单独维护便于 O(1) 读取

	nextID  uint64 // 租约 ID 高水位（分配的最后一个 ID）
	nextLRU uint64 // LRU 序号高水位
	now     int64  // 全局 tick 高水位
	epoch   uint64 // Clear 代数
}

// New 创建容量为 capacity、在途租约上限为 maxInFlight 的缓存。
// 两者都必须是正整数，否则返回 ErrInvalidConfig。
func New(capacity, maxInFlight int) (*Cache, error) {
	if capacity <= 0 || maxInFlight <= 0 {
		return nil, ErrInvalidConfig
	}
	return &Cache{
		capacity:    capacity,
		maxInFlight: maxInFlight,
		states:      make(map[string]*keyState),
		leases:      make(map[uint64]*leaseRec),
	}, nil
}

// checkTick 必须在持有 c.mu 时调用。负值优先判定为 ErrNegativeTick，
// 否则小于高水位判定为 ErrRollback；相等合法。
func (c *Cache) checkTick(now int64) error {
	if now < 0 {
		return ErrNegativeTick
	}
	if now < c.now {
		return ErrRollback
	}
	return nil
}

func (c *Cache) bumpLRU() uint64 {
	c.nextLRU++
	return c.nextLRU
}

// Acquire 尝试获取键 key。
//
//	若存在未过期缓存项：Hit(value)，并刷新该项 LRU；
//	若缓存项恰好过期（now>=expiresAt）：惰性剔除后按未命中处理；
//	若当前分代已有在途租约：Pending(leaseID)（即使在途上限已满也仍可查）；
//	否则成为 Leader 拿到新租约；在途上限已满返回 ErrInFlightLimit。
//
// 成功返回（含三种结果）会把高水位推进到 now；任何错误不改任何状态。
func (c *Cache) Acquire(key string, now int64) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.checkTick(now); err != nil {
		return Result{}, err
	}

	// 以下判定全部是只读的：任何错误返回前都不得改动状态。
	st := c.states[key]
	liveItem := st != nil && st.hasItem && now < st.item.expiresAt
	if liveItem {
		// 命中是成功操作：刷新 LRU 序号并推进高水位。
		st.item.lruSeq = c.bumpLRU()
		c.now = now
		return Result{Kind: KindHit, Value: st.item.value}, nil
	}

	// 未命中。同键同代已有在途租约：Pending 成功（租约不会自动到期）。
	hasLease := st != nil && st.leaseID != 0

	// 新 Leader 路径的两条拒绝理由必须在任何修改之前判定。
	if !hasLease {
		if c.inFlight >= c.maxInFlight {
			return Result{}, ErrInFlightLimit
		}
		if c.nextID == math.MaxUint64 {
			return Result{}, ErrLeaseIDsExhausted
		}
	}

	// 校验全部通过，开始提交。惰性剔除本键恰好过期（now>=expiresAt）的项。
	if st != nil && st.hasItem {
		st.hasItem = false
		st.item = entry{}
		c.nItems--
	}

	if hasLease {
		c.now = now
		return Result{Kind: KindPending, LeaseID: st.leaseID}, nil
	}

	if st == nil {
		st = &keyState{}
		c.states[key] = st
	}
	if st.gen == 0 {
		st.gen = 1
	}
	c.nextID++
	id := c.nextID
	c.leases[id] = &leaseRec{key: key, gen: st.gen}
	st.leaseID = id
	c.inFlight++
	c.now = now

	return Result{
		Kind:    KindLeader,
		Lease:   &Lease{id: id, key: key, gen: st.gen},
		LeaseID: id,
	}, nil
}

// liveLeaseLocked 校验租约仍然存活：索引存在、键状态匹配、分代匹配。
// 调用方必须持有 c.mu，且已通过 checkTick。
func (c *Cache) liveLeaseLocked(lease *Lease) (*keyState, bool) {
	if lease == nil || lease.id == 0 {
		return nil, false
	}
	rec, ok := c.leases[lease.id]
	if !ok || rec.gen != lease.gen || rec.key != lease.key {
		return nil, false
	}
	st := c.states[rec.key]
	if st == nil || st.leaseID != lease.id || st.gen != rec.gen {
		return nil, false
	}
	return st, true
}

// Complete 以租约回填 value，ttl 为正且 now+ttl 不得溢出 int64。
// 成功时结束租约、写入缓存项并刷新该项 LRU；写入前先剔除全部过期项，
// 容量不足再按最小 LRU 序号驱逐。租约 stale 或任何参数错误都不改状态。
func (c *Cache) Complete(lease *Lease, value string, ttl, now int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.checkTick(now); err != nil {
		return err
	}
	st, ok := c.liveLeaseLocked(lease)
	if !ok {
		return ErrStaleLease
	}
	// 溢出判定：ttl > MaxInt64-now 时 now+ttl 越界（now 非负）。
	if ttl <= 0 || ttl > math.MaxInt64-now {
		return ErrInvalidTTL
	}

	// 至此所有校验通过，开始提交（线性化点即持锁区）。

	// 1) 结束租约。
	delete(c.leases, lease.id)
	st.leaseID = 0
	c.inFlight--

	// 2) 先剔除所有已过期缓存项（now>=expiresAt 即过期）。
	for _, s := range c.states {
		if s.hasItem && now >= s.item.expiresAt {
			s.hasItem = false
			s.item = entry{}
			c.nItems--
		}
	}

	// 3) 容量不足再按 LRU 序号驱逐最久未用者，直到能容纳新项。
	for c.nItems >= c.capacity {
		var (
			victimKey string
			victimSeq uint64 = math.MaxUint64
		)
		for k, s := range c.states {
			if s.hasItem && s.item.lruSeq < victimSeq {
				victimKey = k
				victimSeq = s.item.lruSeq
			}
		}
		vs := c.states[victimKey]
		vs.hasItem = false
		vs.item = entry{}
		c.nItems--
	}

	// 4) 写入新项并刷新 LRU。
	st.hasItem = true
	st.item = entry{
		value:     value,
		expiresAt: now + ttl,
		lruSeq:    c.bumpLRU(),
	}
	c.nItems++
	c.now = now
	return nil
}

// Fail 宣告本次加载失败：结束租约但不写缓存。下一次 Acquire 可在同代
// 重新成为 Leader（会拿到新的、不复用的租约 ID）。
func (c *Cache) Fail(lease *Lease, now int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.checkTick(now); err != nil {
		return err
	}
	st, ok := c.liveLeaseLocked(lease)
	if !ok {
		return ErrStaleLease
	}

	delete(c.leases, lease.id)
	st.leaseID = 0
	c.inFlight--
	c.now = now
	return nil
}

// Invalidate 使指定键的缓存项与当前租约失效，并推进该键分代。
// 即使该键当前不存在，也属于成功的修改操作，仍会推进全局高水位。
func (c *Cache) Invalidate(key string, now int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.checkTick(now); err != nil {
		return err
	}

	st := c.states[key]
	if st == nil {
		st = &keyState{}
		c.states[key] = st
	}
	if st.gen == 0 {
		st.gen = 1
	} else {
		st.gen++
	}
	if st.leaseID != 0 {
		delete(c.leases, st.leaseID)
		st.leaseID = 0
		c.inFlight--
	}
	if st.hasItem {
		st.hasItem = false
		st.item = entry{}
		c.nItems--
	}
	c.now = now
	return nil
}

// Clear 清空全部缓存项与全部在途租约并进入新的 Epoch。
// 所有旧租约句柄之后都将得到 ErrStaleLease；租约 ID 计数不复用。
func (c *Cache) Clear(now int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.checkTick(now); err != nil {
		return err
	}

	c.states = make(map[string]*keyState)
	c.leases = make(map[uint64]*leaseRec)
	c.inFlight = 0
	c.nItems = 0
	c.epoch++
	c.now = now
	return nil
}

// Snapshot 返回只读快照；不校验 tick，也不推进时钟。
func (c *Cache) Snapshot() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Items:     c.nItems,
		InFlight:  c.inFlight,
		Keys:      len(c.states),
		HighWater: c.now,
		Epoch:     c.epoch,
	}
}
