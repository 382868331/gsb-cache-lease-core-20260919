package leasecache

import (
	"errors"
	"math"
	"testing"
)

func mustNew(t *testing.T, capacity, maxInFlight int) *Cache {
	t.Helper()
	c, err := New(capacity, maxInFlight)
	if err != nil {
		t.Fatalf("New(%d,%d) unexpected error: %v", capacity, maxInFlight, err)
	}
	return c
}

func isErr(err, target error) bool { return errors.Is(err, target) }

func TestNewRejectsNonPositive(t *testing.T) {
	for _, tc := range [][2]int{{0, 1}, {-1, 1}, {1, 0}, {1, -2}, {0, 0}} {
		if _, err := New(tc[0], tc[1]); !isErr(err, ErrInvalidConfig) {
			t.Fatalf("New(%d,%d) err=%v, want ErrInvalidConfig", tc[0], tc[1], err)
		}
	}
}

// 同 tick：Leader -> Pending -> Complete -> Hit 全部发生在 tick 5。
func TestSameTick(t *testing.T) {
	c := mustNew(t, 4, 4)

	r1, err := c.Acquire("k", 5)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if r1.Kind != KindLeader {
		t.Fatalf("acquire 1 kind=%v, want Leader", r1.Kind)
	}
	if r1.LeaseID != 1 || r1.Lease.ID() != 1 {
		t.Fatalf("unexpected first lease id: %d", r1.LeaseID)
	}

	// 相同 tick 合法（允许相等）。
	r2, err := c.Acquire("k", 5)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if r2.Kind != KindPending || r2.LeaseID != r1.LeaseID {
		t.Fatalf("acquire 2=%+v, want Pending(id=%d)", r2, r1.LeaseID)
	}

	if err := c.Complete(r1.Lease, "v", 10, 5); err != nil {
		t.Fatalf("complete: %v", err)
	}
	r3, err := c.Acquire("k", 5)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if r3.Kind != KindHit || r3.Value != "v" {
		t.Fatalf("acquire 3=%+v, want Hit(v)", r3)
	}

	// Invalidate 后同 tick Acquire 也合法。
	if err := c.Invalidate("k", 5); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	r4, err := c.Acquire("k", 5)
	if err != nil {
		t.Fatalf("acquire 4: %v", err)
	}
	if r4.Kind != KindLeader || r4.LeaseID == r1.LeaseID {
		t.Fatalf("acquire 4=%+v, want new Leader (IDs must not be reused)", r4)
	}
}

// 时间回退：低水位被拒，不产生任何副作用；相等 tick 仍可用。
func TestRollback(t *testing.T) {
	c := mustNew(t, 4, 4)

	if _, err := c.Acquire("k", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := c.Acquire("other", 9); !isErr(err, ErrRollback) {
		t.Fatalf("rollback acquire err=%v, want ErrRollback", err)
	}
	if err := c.Invalidate("k", 9); !isErr(err, ErrRollback) {
		t.Fatalf("rollback invalidate err=%v", err)
	}
	if err := c.Clear(0); !isErr(err, ErrRollback) {
		t.Fatalf("rollback clear err=%v", err)
	}
	if _, err := c.Acquire("neg", -1); !isErr(err, ErrNegativeTick) {
		t.Fatalf("negative tick err=%v", err)
	}

	// 回退未创建任何键/租约。
	st := c.Snapshot()
	if st.Items != 0 || st.InFlight != 1 || st.Keys != 1 || st.HighWater != 10 {
		t.Fatalf("state after rejected ops = %+v", st)
	}

	// 相等 tick 合法。
	r, err := c.Acquire("other", 10)
	if err != nil || r.Kind != KindLeader {
		t.Fatalf("equal-tick acquire: r=%+v err=%v", r, err)
	}
}

// 恰好过期边界：now == expiresAt 即过期；expiresAt-1 仍命中。
func TestExactExpiry(t *testing.T) {
	c := mustNew(t, 4, 4)
	r, _ := c.Acquire("k", 0)
	if err := c.Complete(r.Lease, "v", 5, 0); err != nil { // expiresAt = 5
		t.Fatalf("complete: %v", err)
	}

	hit, err := c.Acquire("k", 4)
	if err != nil || hit.Kind != KindHit || hit.Value != "v" {
		t.Fatalf("at 4: %+v %v", hit, err)
	}
	miss, err := c.Acquire("k", 5)
	if err != nil || miss.Kind != KindLeader {
		t.Fatalf("at 5 (== expiresAt): %+v %v, want Leader", miss, err)
	}
	// 过期项已在成功的 Acquire 中惰性剔除；新租约在途。
	if st := c.Snapshot(); st.Items != 0 || st.InFlight != 1 {
		t.Fatalf("snapshot=%+v", st)
	}
}

// 旧租约回填：Invalidate / Clear 之后旧 Leader 的 Complete/Fail 一律 stale，
// 新分代的值不受影响。
func TestStaleLeaseBackfill(t *testing.T) {
	c := mustNew(t, 4, 4)

	old, _ := c.Acquire("k", 0)
	if err := c.Invalidate("k", 1); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if err := c.Complete(old.Lease, "stale-value", 10, 2); !isErr(err, ErrStaleLease) {
		t.Fatalf("old complete err=%v, want stale", err)
	}
	if err := c.Fail(old.Lease, 2); !isErr(err, ErrStaleLease) {
		t.Fatalf("old fail err=%v, want stale", err)
	}
	if st := c.Snapshot(); st.Items != 0 || st.InFlight != 0 {
		t.Fatalf("stale ops mutated state: %+v", st)
	}

	nr, err := c.Acquire("k", 2)
	if err != nil || nr.Kind != KindLeader {
		t.Fatalf("new acquire: %+v %v", nr, err)
	}
	if nr.Lease.Generation() != old.Lease.Generation()+1 {
		t.Fatalf("generation not bumped: old=%d new=%d", old.Lease.Generation(), nr.Lease.Generation())
	}
	if err := c.Complete(nr.Lease, "fresh", 10, 3); err != nil {
		t.Fatalf("new complete: %v", err)
	}
	hit, _ := c.Acquire("k", 4)
	if hit.Kind != KindHit || hit.Value != "fresh" {
		t.Fatalf("hit=%+v, want fresh value", hit)
	}
	// 旧句柄再来一次，仍然 stale。
	if err := c.Complete(old.Lease, "again", 1, 4); !isErr(err, ErrStaleLease) {
		t.Fatalf("replayed old lease err=%v", err)
	}

	// Clear 后所有租约全部失效。
	l2, _ := c.Acquire("other", 5)
	if err := c.Clear(6); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := c.Complete(l2.Lease, "x", 1, 7); !isErr(err, ErrStaleLease) {
		t.Fatalf("post-clear complete err=%v", err)
	}
	if err := c.Complete(nr.Lease, "y", 1, 7); !isErr(err, ErrStaleLease) {
		t.Fatalf("post-clear completed-again err=%v", err)
	}
	if st := c.Snapshot(); st.Items != 0 || st.InFlight != 0 || st.Keys != 0 || st.Epoch != 1 {
		t.Fatalf("post-clear snapshot=%+v", st)
	}
}

// 租约只由 Complete/Fail/Invalidate 结束：tick 超过它“逻辑加载时长”也仍然存活。
func TestLeaseDoesNotExpireByTime(t *testing.T) {
	c := mustNew(t, 4, 1)
	r, _ := c.Acquire("k", 0)
	// 很久以后，租约仍在途，新 Leader 受在途计数约束；但同键仍可查 Pending。
	if _, err := c.Acquire("other", 1000); !isErr(err, ErrInFlightLimit) {
		t.Fatalf("expected in-flight limit, got %v", err)
	}
	p, err := c.Acquire("k", 1000)
	if err != nil || p.Kind != KindPending || p.LeaseID != r.LeaseID {
		t.Fatalf("pending lookup: %+v %v", p, err)
	}
	if err := c.Complete(r.Lease, "late", 1, 1000); err != nil {
		t.Fatalf("late complete should still succeed: %v", err)
	}
}

// LRU：容量 1，后写驱逐先写；Hit 刷新序号；写入前先剔除过期项。
func TestLRUCapacityOne(t *testing.T) {
	c := mustNew(t, 1, 4)

	put := func(key, value string, now, ttl int64) *Lease {
		r, err := c.Acquire(key, now)
		if err != nil {
			t.Fatalf("acquire %s: %v", key, err)
		}
		if r.Kind != KindLeader {
			t.Fatalf("acquire %s kind=%v", key, r.Kind)
		}
		if err := c.Complete(r.Lease, value, ttl, now); err != nil {
			t.Fatalf("complete %s: %v", key, err)
		}
		return r.Lease
	}

	put("a", "A", 0, 100)
	put("b", "B", 1, 100) // 驱逐 a
	if h, _ := c.Acquire("a", 2); h.Kind != KindLeader {
		t.Fatalf("a should have been evicted, got %+v", h)
	}
	if st := c.Snapshot(); st.Items != 1 {
		t.Fatalf("items=%d, want 1", st.Items)
	}
	// 此时 b 在缓存；先完成 a 的回填（会驱逐 b），再 Hit b 之前……直接验证序号定序：
	// a 刚写入；写入 c 应驱逐 a 而不是 b？容量 1 下只剩 a，预期驱逐 a。
	put("c", "C", 3, 100)
	if h, _ := c.Acquire("b", 4); h.Kind != KindLeader {
		t.Fatalf("b should have been evicted by c (cap=1), got %+v", h)
	}
}

func TestLRUHitRefreshesOrder(t *testing.T) {
	c := mustNew(t, 2, 4)
	put := func(key, value string, now int64) {
		t.Helper()
		r, err := c.Acquire(key, now)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if err := c.Complete(r.Lease, value, 1000, now); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}
	put("a", "A", 0)
	put("b", "B", 1)
	if h, err := c.Acquire("a", 2); err != nil || h.Kind != KindHit { // 刷新 a
		t.Fatalf("hit a: %+v %v", h, err)
	}
	put("c", "C", 3) // 应驱逐 b（最久未用），保留 a
	if h, _ := c.Acquire("a", 4); h.Kind != KindHit {
		t.Fatalf("a must survive LRU refresh, got %+v", h)
	}
	if h, _ := c.Acquire("b", 4); h.Kind != KindLeader {
		t.Fatalf("b must be evicted, got %+v", h)
	}
}

// 写入时先剔除过期项再驱逐：容量 1，旧项恰好过期时新写入不应触发 LRU 驱逐分支，
// 且与过期项无关的另一过期项也会被一并清扫（容量 2 的情形）。
func TestExpireBeforeEvict(t *testing.T) {
	c := mustNew(t, 2, 4)
	load := func(key, value string, now, ttl int64) {
		t.Helper()
		r, err := c.Acquire(key, now)
		if err != nil {
			t.Fatalf("acquire %s: %v", key, err)
		}
		if err := c.Complete(r.Lease, value, ttl, now); err != nil {
			t.Fatalf("complete %s: %v", key, err)
		}
	}
	load("a", "A", 0, 5) // expiresAt 5
	load("b", "B", 0, 10)

	// now=10：a 过期；Complete c 前的清扫同时清掉恰好过期(now==expiresAt)的 a。
	// b 在 now=10 也恰好过期，一并清扫。
	r, err := c.Acquire("c", 10)
	if err != nil || r.Kind != KindLeader {
		t.Fatalf("acquire c: %+v %v", r, err)
	}
	if err := c.Complete(r.Lease, "C", 100, 10); err != nil {
		t.Fatalf("complete c: %v", err)
	}
	if st := c.Snapshot(); st.Items != 1 {
		t.Fatalf("items=%d, want 1 (only c)", st.Items)
	}
	if h, _ := c.Acquire("c", 11); h.Kind != KindHit || h.Value != "C" {
		t.Fatalf("c hit: %+v", h)
	}
}

// 在途上限：满额拒绝新 Leader，但既有 Pending 仍可查；结束一个租约后恢复。
func TestInFlightLimit(t *testing.T) {
	c := mustNew(t, 8, 2)
	l1, err := c.Acquire("k1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("k2", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("k3", 0); !isErr(err, ErrInFlightLimit) {
		t.Fatalf("third leader err=%v, want ErrInFlightLimit", err)
	}
	// 上限拒绝不改变在途计数。
	if st := c.Snapshot(); st.InFlight != 2 {
		t.Fatalf("inflight=%d after rejection, want 2", st.InFlight)
	}
	// 现有 Pending 仍可查（不受上限影响）。
	p, err := c.Acquire("k1", 1)
	if err != nil || p.Kind != KindPending || p.LeaseID != l1.LeaseID {
		t.Fatalf("pending lookup at limit: %+v %v", p, err)
	}

	if err := c.Fail(l1.Lease, 2); err != nil {
		t.Fatalf("fail: %v", err)
	}
	r3, err := c.Acquire("k3", 3)
	if err != nil || r3.Kind != KindLeader {
		t.Fatalf("leader after fail: %+v %v", r3, err)
	}
}

// 非法 TTL：ttl<=0 与溢出都被拒；且租约不被消耗，仍可正常 Complete。
func TestInvalidTTL(t *testing.T) {
	c := mustNew(t, 4, 4)
	r, _ := c.Acquire("k", 0)

	for _, ttl := range []int64{0, -1, -100, math.MaxInt64} {
		if err := c.Complete(r.Lease, "v", ttl, 1); !isErr(err, ErrInvalidTTL) {
			t.Fatalf("ttl=%d err=%v, want ErrInvalidTTL", ttl, err)
		}
	}
	// now 很大时，即使 ttl 较小也可能溢出。
	big := r
	if err := c.Complete(big.Lease, "v", math.MaxInt64-5+1, 5); !isErr(err, ErrInvalidTTL) {
		t.Fatalf("boundary overflow ttl err=%v", err)
	}
	// 边界合法值：ttl = MaxInt64-now。
	if err := c.Complete(r.Lease, "ok", math.MaxInt64-5, 5); err != nil {
		t.Fatalf("boundary-valid ttl: %v", err)
	}
	if h, _ := c.Acquire("k", 6); h.Kind != KindHit {
		t.Fatalf("hit after valid complete: %+v", h)
	}
}

// 任意错误不推进时钟、不改数据/LRU/租约。
func TestErrorsDoNotAdvanceClockOrState(t *testing.T) {
	c := mustNew(t, 2, 1)
	r, _ := c.Acquire("k", 7)
	if err := c.Complete(r.Lease, "v", 100, 7); err != nil {
		t.Fatal(err)
	}
	la, err := c.Acquire("a", 7) // 占满唯一的在途名额
	if err != nil {
		t.Fatal(err)
	}
	before := c.Snapshot()

	// 时间回退错误（即便 k 会命中，也必须先判时钟，不得刷新 LRU）。
	if _, err := c.Acquire("k", 6); !isErr(err, ErrRollback) {
		t.Fatalf("rollback: %v", err)
	}
	// stale 错误（旧租约已结束）。
	if err := c.Complete(r.Lease, "v2", 10, 7); !isErr(err, ErrStaleLease) {
		t.Fatalf("stale: %v", err)
	}
	// 未知租约。
	if err := c.Fail(&Lease{id: 999, key: "k", gen: 1}, 7); !isErr(err, ErrStaleLease) {
		t.Fatalf("unknown lease: %v", err)
	}
	// 在途上限错误。
	if _, err := c.Acquire("b", 7); !isErr(err, ErrInFlightLimit) {
		t.Fatalf("limit: %v", err)
	}
	// 非法 TTL 错误（在途租约不被消耗）。
	if err := c.Complete(la.Lease, "x", 0, 7); !isErr(err, ErrInvalidTTL) {
		t.Fatalf("ttl=0: %v", err)
	}

	after := c.Snapshot()
	if before != after {
		t.Fatalf("state changed on errors:\nbefore=%+v\nafter =%+v", before, after)
	}

	// 租约在非法 TTL 后仍存活，可正常回填。
	p, err := c.Acquire("a", 7)
	if err != nil || p.Kind != KindPending {
		t.Fatalf("lease must survive invalid ttl, got %+v %v", p, err)
	}
	if err := c.Complete(la.Lease, "real", 10, 7); err != nil {
		t.Fatalf("complete after rejected ttl: %v", err)
	}
}

// 只读快照不推进时钟。
func TestSnapshotDoesNotAdvanceClock(t *testing.T) {
	c := mustNew(t, 4, 4)
	_, _ = c.Acquire("k", 5)
	_ = c.Snapshot()
	_ = c.Snapshot()
	if st := c.Snapshot(); st.HighWater != 5 {
		t.Fatalf("highwater=%d, want 5", st.HighWater)
	}
	// 快照之后 tick=5 仍可用（没有被快照推进）。
	if _, err := c.Acquire("k2", 5); err != nil {
		t.Fatalf("equal tick after snapshots rejected: %v", err)
	}
}

// 租约 ID 不复用：Fail 之后新租约取新 ID。
func TestLeaseIDsNotReused(t *testing.T) {
	c := mustNew(t, 4, 4)
	r1, _ := c.Acquire("k", 0)
	if err := c.Fail(r1.Lease, 0); err != nil {
		t.Fatal(err)
	}
	r2, _ := c.Acquire("k", 0)
	if r2.LeaseID == r1.LeaseID {
		t.Fatalf("lease id reused: %d", r1.LeaseID)
	}
	if r2.LeaseID != r1.LeaseID+1 {
		t.Fatalf("ids not monotonic: %d -> %d", r1.LeaseID, r2.LeaseID)
	}
}
