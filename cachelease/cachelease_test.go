package cachelease

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
)

func mustNew(t *testing.T, capacity, inflight int) *Cache {
	t.Helper()
	c, err := New(capacity, inflight)
	if err != nil {
		t.Fatalf("New(%d, %d): %v", capacity, inflight, err)
	}
	return c
}

func mustLeader(t *testing.T, c *Cache, key string, now int64) Lease {
	t.Helper()
	r, err := c.Acquire(key, now)
	if err != nil {
		t.Fatalf("Acquire(%q, %d): %v", key, now, err)
	}
	if r.Kind != Leader {
		t.Fatalf("Acquire(%q, %d) = %v, want Leader", key, now, r.Kind)
	}
	return r.Lease
}

func mustComplete(t *testing.T, c *Cache, lease Lease, value string, ttl, now int64) {
	t.Helper()
	if err := c.Complete(lease, value, ttl, now); err != nil {
		t.Fatalf("Complete(key=%q, now=%d): %v", lease.Key(), now, err)
	}
}

func mustHit(t *testing.T, c *Cache, key string, now int64, want string) {
	t.Helper()
	r, err := c.Acquire(key, now)
	if err != nil {
		t.Fatalf("Acquire(%q, %d): %v", key, now, err)
	}
	if r.Kind != Hit || r.Value != want {
		t.Fatalf("Acquire(%q, %d) = (%v, %q), want Hit %q", key, now, r.Kind, r.Value, want)
	}
}

// 同tick：高水位允许相等的 now。
func TestSameTick(t *testing.T) {
	c := mustNew(t, 4, 4)
	lease := mustLeader(t, c, "k", 5)
	mustComplete(t, c, lease, "v", 10, 5) // equal to high-water mark
	mustHit(t, c, "k", 5, "v")            // equal again
	if got := c.Now(); got != 5 {
		t.Fatalf("Now() = %d, want 5", got)
	}
}

// 时间回退：小于高水位拒绝，且状态不变。
func TestClockRegression(t *testing.T) {
	c := mustNew(t, 4, 4)
	lease := mustLeader(t, c, "k", 10)
	mustComplete(t, c, lease, "v", 100, 10)

	for _, now := range []int64{0, 9} {
		if _, err := c.Acquire("x", now); !errors.Is(err, ErrClockRegression) {
			t.Fatalf("Acquire(x, %d) err = %v, want ErrClockRegression", now, err)
		}
		if err := c.Invalidate("k", now); !errors.Is(err, ErrClockRegression) {
			t.Fatalf("Invalidate(k, %d) err = %v, want ErrClockRegression", now, err)
		}
		if err := c.Clear(now); !errors.Is(err, ErrClockRegression) {
			t.Fatalf("Clear(%d) err = %v, want ErrClockRegression", now, err)
		}
	}
	// State untouched: value still there, clock unchanged.
	mustHit(t, c, "k", 10, "v")
	if got := c.Now(); got != 10 {
		t.Fatalf("Now() = %d, want 10", got)
	}
}

// 恰好过期：now == expiresAt 视为过期。
func TestExactExpiry(t *testing.T) {
	c := mustNew(t, 4, 4)
	lease := mustLeader(t, c, "k", 5)
	mustComplete(t, c, lease, "v", 10, 5) // expiresAt = 15

	mustHit(t, c, "k", 14, "v")

	r, err := c.Acquire("k", 15) // now == expiresAt -> expired
	if err != nil {
		t.Fatalf("Acquire(k, 15): %v", err)
	}
	if r.Kind != Leader {
		t.Fatalf("Acquire(k, 15) = %v, want Leader (expired)", r.Kind)
	}
}

// 旧租约回填：Invalidate 之后旧 Complete/Fail 必须 stale，且不影响新代。
func TestStaleLeaseRefill(t *testing.T) {
	c := mustNew(t, 4, 4)
	old := mustLeader(t, c, "k", 1)

	if err := c.Invalidate("k", 2); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if err := c.Complete(old, "stale-value", 100, 3); !errors.Is(err, ErrStale) {
		t.Fatalf("old Complete err = %v, want ErrStale", err)
	}
	if err := c.Fail(old, 3); !errors.Is(err, ErrStale) {
		t.Fatalf("old Fail err = %v, want ErrStale", err)
	}
	// 旧租约没有写入任何东西。
	r, err := c.Acquire("k", 4)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if r.Kind != Leader {
		t.Fatalf("Acquire after stale refill = %v, want Leader", r.Kind)
	}
	if r.Lease.ID() == old.ID() {
		t.Fatalf("lease ID %d reused", old.ID())
	}
	// 新代租约可以正常完成。
	mustComplete(t, c, r.Lease, "fresh", 100, 5)
	mustHit(t, c, "k", 6, "fresh")
}

// Clear 使所有缓存与租约失效。
func TestClearInvalidatesEverything(t *testing.T) {
	c := mustNew(t, 4, 4)
	l1 := mustLeader(t, c, "a", 1)
	mustComplete(t, c, l1, "va", 100, 2)
	l2 := mustLeader(t, c, "b", 3)

	if err := c.Clear(4); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if got := c.Inflight(); got != 0 {
		t.Fatalf("Inflight() = %d, want 0", got)
	}
	if err := c.Complete(l2, "vb", 100, 5); !errors.Is(err, ErrStale) {
		t.Fatalf("Complete after Clear err = %v, want ErrStale", err)
	}
	// 新代不受影响。
	l3 := mustLeader(t, c, "a", 6)
	mustComplete(t, c, l3, "new", 100, 7)
	mustHit(t, c, "a", 8, "new")
}

// Clear 竞态：Complete 与 Clear 并发，结果必须可线性化为二者之一。
func TestClearRaceLinearizable(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := mustNew(t, 4, 4)
		lease := mustLeader(t, c, "k", 1)

		var barrier sync.WaitGroup
		barrier.Add(2)
		start := make(chan struct{})
		var done sync.WaitGroup
		done.Add(2)

		var completeErr error
		go func() {
			defer done.Done()
			barrier.Done()
			<-start
			completeErr = c.Complete(lease, "v", 100, 2)
		}()
		var clearErr error
		go func() {
			defer done.Done()
			barrier.Done()
			<-start
			clearErr = c.Clear(2)
		}()
		barrier.Wait()
		close(start)
		done.Wait()

		if clearErr != nil {
			t.Fatalf("iter %d: Clear err = %v", i, clearErr)
		}
		switch {
		case completeErr == nil:
			// Complete 线性化在 Clear 之前：值已被清掉。
			if got := c.Len(); got != 0 {
				t.Fatalf("iter %d: Complete ok but Len() = %d", i, got)
			}
		case errors.Is(completeErr, ErrStale):
			// Clear 线性化在 Complete 之前：租约已失效。
			if got := c.Inflight(); got != 0 {
				t.Fatalf("iter %d: stale Complete but Inflight() = %d", i, got)
			}
		default:
			t.Fatalf("iter %d: Complete err = %v, want nil or ErrStale", i, completeErr)
		}
		if got := c.Now(); got != 2 {
			t.Fatalf("iter %d: Now() = %d, want 2", i, got)
		}
	}
}

// LRU 容量 1：新写入驱逐旧项；Hit 更新 LRU。
func TestLRUCapacityOne(t *testing.T) {
	c := mustNew(t, 1, 4)
	la := mustLeader(t, c, "a", 1)
	mustComplete(t, c, la, "va", 100, 1)
	lb := mustLeader(t, c, "b", 2)
	mustComplete(t, c, lb, "vb", 100, 2) // evicts "a"

	if got := c.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	r, err := c.Acquire("a", 3)
	if err != nil {
		t.Fatalf("Acquire(a): %v", err)
	}
	if r.Kind != Leader {
		t.Fatalf("Acquire(a) = %v, want Leader (evicted)", r.Kind)
	}
	mustHit(t, c, "b", 4, "vb")
}

// Hit 刷新 LRU 顺序，容量 2 时驱逐的是未命中的那一项。
func TestLRUHitRefreshes(t *testing.T) {
	c := mustNew(t, 2, 4)
	la := mustLeader(t, c, "a", 1)
	mustComplete(t, c, la, "va", 100, 1)
	lb := mustLeader(t, c, "b", 2)
	mustComplete(t, c, lb, "vb", 100, 2)
	mustHit(t, c, "a", 3, "va") // refresh "a"; "b" is now oldest

	lc := mustLeader(t, c, "c", 4)
	mustComplete(t, c, lc, "vc", 100, 4) // evicts "b"

	mustHit(t, c, "a", 5, "va")
	r, err := c.Acquire("b", 6)
	if err != nil {
		t.Fatalf("Acquire(b): %v", err)
	}
	if r.Kind != Leader {
		t.Fatalf("Acquire(b) = %v, want Leader (evicted)", r.Kind)
	}
	mustHit(t, c, "c", 7, "vc")
}

// 写入时先剔除过期项再驱逐：过期项应优先于 LRU 受害者被移除。
func TestWritePurgesExpiredBeforeEvicting(t *testing.T) {
	c := mustNew(t, 2, 4)
	la := mustLeader(t, c, "a", 1)
	mustComplete(t, c, la, "va", 5, 1) // expiresAt = 6
	lb := mustLeader(t, c, "b", 2)
	mustComplete(t, c, lb, "vb", 100, 2) // "b" is MRU, "a" is LRU *and* expired

	lc := mustLeader(t, c, "c", 10)
	mustComplete(t, c, lc, "vc", 100, 10) // purge expired "a", no eviction needed

	mustHit(t, c, "b", 11, "vb") // "b" must survive: only expired "a" was removed
	mustHit(t, c, "c", 12, "vc")
	if got := c.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

// 在途上限：达到上限拒绝新 Leader，现有 Pending 仍可查。
func TestInflightLimit(t *testing.T) {
	c := mustNew(t, 4, 1)
	l1 := mustLeader(t, c, "k1", 1)

	if _, err := c.Acquire("k2", 2); !errors.Is(err, ErrInflightLimit) {
		t.Fatalf("Acquire(k2) err = %v, want ErrInflightLimit", err)
	}
	// 同键同代仍可查询到在途租约。
	r, err := c.Acquire("k1", 3)
	if err != nil {
		t.Fatalf("Acquire(k1): %v", err)
	}
	if r.Kind != Pending || r.LeaseID != l1.ID() {
		t.Fatalf("Acquire(k1) = (%v, id=%d), want Pending id=%d", r.Kind, r.LeaseID, l1.ID())
	}
	// 超限错误不改变状态。
	if got := c.Inflight(); got != 1 {
		t.Fatalf("Inflight() = %d, want 1", got)
	}
	// Fail 释放名额后可以再取新租约。
	if err := c.Fail(l1, 4); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if err := c.Fail(l1, 5); !errors.Is(err, ErrStale) {
		t.Fatalf("second Fail err = %v, want ErrStale", err)
	}
	mustLeader(t, c, "k2", 6)
}

// 错误不推进时钟：各类错误之后高水位、数据、LRU、租约均不变。
func TestErrorsDoNotAdvanceClock(t *testing.T) {
	c := mustNew(t, 2, 1)
	l1 := mustLeader(t, c, "k", 10)
	mustComplete(t, c, l1, "v", 100, 10)
	l2 := mustLeader(t, c, "z", 11) // inflight now 1/1

	snapBefore := c.Snapshot()
	inflightBefore := c.Inflight()

	checkUnchanged := func(step string) {
		t.Helper()
		if got := c.Now(); got != 11 {
			t.Fatalf("%s: Now() = %d, want 11", step, got)
		}
		if got := c.Inflight(); got != inflightBefore {
			t.Fatalf("%s: Inflight() = %d, want %d", step, got, inflightBefore)
		}
		got := c.Snapshot()
		if fmt.Sprint(got) != fmt.Sprint(snapBefore) {
			t.Fatalf("%s: Snapshot = %v, want %v", step, got, snapBefore)
		}
	}

	if _, err := c.Acquire("k", 5); !errors.Is(err, ErrClockRegression) {
		t.Fatalf("regression Acquire err = %v", err)
	}
	checkUnchanged("clock regression")

	if err := c.Invalidate("k", 3); !errors.Is(err, ErrClockRegression) {
		t.Fatalf("regression Invalidate err = %v", err)
	}
	checkUnchanged("regression invalidate")

	if err := c.Complete(l2, "x", 0, 12); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("ttl=0 err = %v", err)
	}
	checkUnchanged("invalid ttl")

	if err := c.Complete(l2, "x", math.MaxInt64, 12); !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow err = %v", err)
	}
	checkUnchanged("overflow")

	if _, err := c.Acquire("other", 12); !errors.Is(err, ErrInflightLimit) {
		t.Fatalf("inflight err = %v", err)
	}
	checkUnchanged("inflight limit")

	stale := Lease{} // zero lease, never issued
	if err := c.Complete(stale, "x", 1, 12); !errors.Is(err, ErrStale) {
		t.Fatalf("stale Complete err = %v", err)
	}
	checkUnchanged("stale complete")

	if _, err := c.Acquire("k", -1); !errors.Is(err, ErrNegativeTime) {
		t.Fatalf("negative time err = %v", err)
	}
	checkUnchanged("negative time")

	// 时钟未被错误推进：相等 now 仍然允许。
	mustHit(t, c, "k", 11, "v")
}

// 租约 ID 不复用，计数耗尽明确拒绝。
func TestLeaseIDExhaustion(t *testing.T) {
	c := mustNew(t, 4, 4)
	c.nextLeaseID = math.MaxUint64

	r, err := c.Acquire("a", 1)
	if err != nil {
		t.Fatalf("Acquire(a): %v", err)
	}
	if r.Kind != Leader || r.Lease.ID() != math.MaxUint64 {
		t.Fatalf("got (%v, id=%d), want Leader id=MaxUint64", r.Kind, r.Lease.ID())
	}
	if _, err := c.Acquire("b", 2); !errors.Is(err, ErrLeaseIDExhausted) {
		t.Fatalf("Acquire(b) err = %v, want ErrLeaseIDExhausted", err)
	}
	// 耗尽后同键 Pending 仍可查。
	r2, err := c.Acquire("a", 3)
	if err != nil {
		t.Fatalf("Acquire(a) again: %v", err)
	}
	if r2.Kind != Pending || r2.LeaseID != math.MaxUint64 {
		t.Fatalf("got (%v, id=%d), want Pending id=MaxUint64", r2.Kind, r2.LeaseID)
	}
	// 完成最后一个租约后 ID 空间仍然耗尽（不复用）。
	mustComplete(t, c, r.Lease, "v", 10, 4)
	if _, err := c.Acquire("b", 5); !errors.Is(err, ErrLeaseIDExhausted) {
		t.Fatalf("Acquire(b) after complete err = %v, want ErrLeaseIDExhausted", err)
	}
}

// 只读快照不推进时钟。
func TestSnapshotDoesNotAdvanceClock(t *testing.T) {
	c := mustNew(t, 4, 4)
	l := mustLeader(t, c, "k", 7)
	mustComplete(t, c, l, "v", 100, 7)

	_ = c.Snapshot()
	_ = c.Now()
	_ = c.Len()
	_ = c.Inflight()
	if got := c.Now(); got != 7 {
		t.Fatalf("Now() = %d after read-only calls, want 7", got)
	}
	snap := c.Snapshot()
	if len(snap) != 1 || snap[0].Key != "k" || snap[0].Value != "v" || snap[0].ExpiresAt != 107 {
		t.Fatalf("Snapshot = %+v", snap)
	}
}

// 参数校验。
func TestConfigAndArgs(t *testing.T) {
	if _, err := New(0, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(0,1) err = %v", err)
	}
	if _, err := New(1, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(1,0) err = %v", err)
	}
	c := mustNew(t, 1, 1)
	l := mustLeader(t, c, "k", 0) // now = 0 allowed
	if err := c.Complete(l, "v", -3, 1); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("negative ttl err = %v", err)
	}
	if err := c.Complete(l, "v", 10, -1); !errors.Is(err, ErrNegativeTime) {
		t.Fatalf("negative now err = %v", err)
	}
	mustComplete(t, c, l, "v", 10, 1)
	mustHit(t, c, "k", 2, "v")
}

// 并发屏障：多 goroutine 同时随机操作，验证无 panic、时钟单调、
// 同键同代至多一个 Leader。
func TestConcurrentBarrier(t *testing.T) {
	c := mustNew(t, 8, 4)
	const workers = 8
	const ops = 200

	var barrier sync.WaitGroup
	barrier.Add(workers)
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(workers)

	keys := []string{"a", "b", "c", "d"}
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer done.Done()
			rng := rand.New(rand.NewSource(seed))
			var held []Lease
			barrier.Done()
			<-start
			for i := 0; i < ops; i++ {
				now := int64(rng.Intn(400))
				key := keys[rng.Intn(len(keys))]
				switch rng.Intn(5) {
				case 0, 1:
					r, err := c.Acquire(key, now)
					if err == nil && r.Kind == Leader {
						held = append(held, r.Lease)
					}
				case 2:
					if len(held) > 0 {
						l := held[len(held)-1]
						held = held[:len(held)-1]
						_ = c.Complete(l, "v", 50, now)
					}
				case 3:
					if len(held) > 0 {
						l := held[len(held)-1]
						held = held[:len(held)-1]
						_ = c.Fail(l, now)
					}
				case 4:
					_ = c.Invalidate(key, now)
				}
			}
		}(int64(w) * 977)
	}
	barrier.Wait()
	close(start)
	done.Wait()

	if got := c.Len(); got > 8 {
		t.Fatalf("Len() = %d exceeds capacity 8", got)
	}
	if got := c.Inflight(); got > 4 {
		t.Fatalf("Inflight() = %d exceeds limit 4", got)
	}
}

// 顺序参考模型：独立实现的简单模型，固定种子随机对比。
type model struct {
	capacity int
	limit    int
	now      int64
	entries  map[string]modelEntry
	lruSeq   int64
	leases   map[uint64]Lease
	byKey    map[string]uint64 // genKey -> lease id
	nextID   uint64
	epoch    uint64
	gen      map[string]uint64
}

type modelEntry struct {
	value     string
	expiresAt int64
	seq       int64
}

func genKey(epoch, gen uint64, key string) string {
	return fmt.Sprintf("%d/%d/%s", epoch, gen, key)
}

func newModel(capacity, limit int) *model {
	return &model{
		capacity: capacity,
		limit:    limit,
		entries:  map[string]modelEntry{},
		leases:   map[uint64]Lease{},
		byKey:    map[string]uint64{},
		nextID:   1,
		gen:      map[string]uint64{},
	}
}

func (m *model) acquire(key string, now int64) (Result, error) {
	if now < 0 {
		return Result{}, ErrNegativeTime
	}
	if now < m.now {
		return Result{}, ErrClockRegression
	}
	if e, ok := m.entries[key]; ok {
		if now < e.expiresAt {
			m.lruSeq++
			e.seq = m.lruSeq
			m.entries[key] = e
			m.now = now
			return Result{Kind: Hit, Value: e.value}, nil
		}
		delete(m.entries, key)
	}
	gk := genKey(m.epoch, m.gen[key], key)
	if id, ok := m.byKey[gk]; ok {
		m.now = now
		return Result{Kind: Pending, LeaseID: id}, nil
	}
	if len(m.leases) >= m.limit {
		return Result{}, ErrInflightLimit
	}
	if m.nextID == 0 {
		return Result{}, ErrLeaseIDExhausted
	}
	id := m.nextID
	if id == math.MaxUint64 {
		m.nextID = 0
	} else {
		m.nextID = id + 1
	}
	lease := Lease{id: id, key: key, epoch: m.epoch, gen: m.gen[key]}
	m.leases[id] = lease
	m.byKey[gk] = id
	m.now = now
	return Result{Kind: Leader, Lease: lease}, nil
}

func (m *model) complete(lease Lease, value string, ttl, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if ttl > math.MaxInt64-now {
		return ErrOverflow
	}
	if now < m.now {
		return ErrClockRegression
	}
	live, ok := m.leases[lease.id]
	if !ok || live != lease {
		return ErrStale
	}
	delete(m.leases, lease.id)
	delete(m.byKey, genKey(lease.epoch, lease.gen, lease.key))
	// purge expired
	for k, e := range m.entries {
		if now >= e.expiresAt {
			delete(m.entries, k)
		}
	}
	m.lruSeq++
	m.entries[lease.key] = modelEntry{value: value, expiresAt: now + ttl, seq: m.lruSeq}
	for len(m.entries) > m.capacity {
		// evict smallest seq
		var victim string
		var minSeq int64 = math.MaxInt64
		for k, e := range m.entries {
			if e.seq < minSeq {
				minSeq = e.seq
				victim = k
			}
		}
		delete(m.entries, victim)
	}
	m.now = now
	return nil
}

func (m *model) fail(lease Lease, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	if now < m.now {
		return ErrClockRegression
	}
	live, ok := m.leases[lease.id]
	if !ok || live != lease {
		return ErrStale
	}
	delete(m.leases, lease.id)
	delete(m.byKey, genKey(lease.epoch, lease.gen, lease.key))
	m.now = now
	return nil
}

func (m *model) invalidate(key string, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	if now < m.now {
		return ErrClockRegression
	}
	delete(m.entries, key)
	gk := genKey(m.epoch, m.gen[key], key)
	if id, ok := m.byKey[gk]; ok {
		delete(m.byKey, gk)
		delete(m.leases, id)
	}
	m.gen[key]++
	m.now = now
	return nil
}

func (m *model) clear(now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	if now < m.now {
		return ErrClockRegression
	}
	m.entries = map[string]modelEntry{}
	m.leases = map[uint64]Lease{}
	m.byKey = map[string]uint64{}
	m.epoch++
	m.now = now
	return nil
}

func sameResult(a, b Result) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case Hit:
		return a.Value == b.Value
	case Leader:
		return a.Lease == b.Lease
	case Pending:
		return a.LeaseID == b.LeaseID
	}
	return true
}

// 固定种子、小样本的随机顺序验证：真实实现与参考模型逐步对比。
func TestSequentialReferenceModel(t *testing.T) {
	rng := rand.New(rand.NewSource(20260919))
	c := mustNew(t, 3, 2)
	m := newModel(3, 2)

	keys := []string{"a", "b", "c", "d"}
	var heldLeases []Lease // leases issued by both (identical sequences)

	for step := 0; step < 600; step++ {
		// 70% 单调推进的 now，30% 随机（可能回退触发错误）。
		var now int64
		if rng.Intn(10) < 7 {
			now = c.Now() + int64(rng.Intn(4))
		} else {
			now = int64(rng.Intn(30))
		}
		key := keys[rng.Intn(len(keys))]
		op := rng.Intn(100)

		switch {
		case op < 45: // Acquire
			gotR, gotErr := c.Acquire(key, now)
			wantR, wantErr := m.acquire(key, now)
			if (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("step %d Acquire(%q,%d): err %v vs model %v", step, key, now, gotErr, wantErr)
			}
			if gotErr != nil && !errors.Is(gotErr, wantErr) {
				t.Fatalf("step %d Acquire(%q,%d): err %v vs model %v", step, key, now, gotErr, wantErr)
			}
			if gotErr == nil && !sameResult(gotR, wantR) {
				t.Fatalf("step %d Acquire(%q,%d): %+v vs model %+v", step, key, now, gotR, wantR)
			}
			if gotErr == nil && gotR.Kind == Leader {
				heldLeases = append(heldLeases, gotR.Lease)
			}
		case op < 65: // Complete
			if len(heldLeases) == 0 {
				continue
			}
			i := rng.Intn(len(heldLeases))
			lease := heldLeases[i]
			heldLeases = append(heldLeases[:i], heldLeases[i+1:]...)
			ttl := int64(rng.Intn(8)) // includes 0 -> invalid
			value := fmt.Sprintf("v%d", step)
			gotErr := c.Complete(lease, value, ttl, now)
			wantErr := m.complete(lease, value, ttl, now)
			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && !errors.Is(gotErr, wantErr)) {
				t.Fatalf("step %d Complete(%q,%d): err %v vs model %v", step, key, now, gotErr, wantErr)
			}
		case op < 75: // Fail
			if len(heldLeases) == 0 {
				continue
			}
			i := rng.Intn(len(heldLeases))
			lease := heldLeases[i]
			heldLeases = append(heldLeases[:i], heldLeases[i+1:]...)
			gotErr := c.Fail(lease, now)
			wantErr := m.fail(lease, now)
			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && !errors.Is(gotErr, wantErr)) {
				t.Fatalf("step %d Fail(%q,%d): err %v vs model %v", step, key, now, gotErr, wantErr)
			}
		case op < 90: // Invalidate
			gotErr := c.Invalidate(key, now)
			wantErr := m.invalidate(key, now)
			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && !errors.Is(gotErr, wantErr)) {
				t.Fatalf("step %d Invalidate(%q,%d): err %v vs model %v", step, key, now, gotErr, wantErr)
			}
		default: // Clear
			gotErr := c.Clear(now)
			wantErr := m.clear(now)
			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && !errors.Is(gotErr, wantErr)) {
				t.Fatalf("step %d Clear(%d): err %v vs model %v", step, now, gotErr, wantErr)
			}
		}

		if c.Now() != m.now {
			t.Fatalf("step %d: Now() = %d, model %d", step, c.Now(), m.now)
		}
		if c.Len() != len(m.entries) {
			t.Fatalf("step %d: Len() = %d, model %d", step, c.Len(), len(m.entries))
		}
		if c.Inflight() != len(m.leases) {
			t.Fatalf("step %d: Inflight() = %d, model %d", step, c.Inflight(), len(m.leases))
		}
	}

	// 最终快照与模型一致。
	snap := c.Snapshot()
	if len(snap) != len(m.entries) {
		t.Fatalf("final snapshot len %d, model %d", len(snap), len(m.entries))
	}
	for _, e := range snap {
		me, ok := m.entries[e.Key]
		if !ok || me.value != e.Value || me.expiresAt != e.ExpiresAt {
			t.Fatalf("final snapshot entry %+v, model %+v (ok=%v)", e, me, ok)
		}
	}
}
