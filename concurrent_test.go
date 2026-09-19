package leasecache

import (
	"errors"
	"sync"
	"testing"
)

// 本文件使用同步屏障（channel / WaitGroup rendezvous）构造确定性的
// 失效/完成竞态，不使用 sleep 猜测时序。

// invalidateThenCompleteRace：两个 goroutine 在屏障处同时释放，一个执行
// Invalidate，一个执行旧租约 Complete。无论线性化顺序如何：
//   - 恰好一个操作的语义结果成立；
//   - 旧值绝不会在 Invalidate 已线性化之后残留（stale 回填被拒绝）；
//   - 最终缓存中要么是新值（Complete 先于 Invalidate 且随后重新加载），
//     要么为空，但绝不会是旧值。
func TestConcurrentInvalidateCompleteBarrier(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		c := mustNew(t, 4, 4)
		r, err := c.Acquire("k", int64(iter))
		if err != nil {
			t.Fatal(err)
		}
		base := int64(iter) + 1

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var completeErr error
		go func() { // completer
			defer wg.Done()
			<-start
			completeErr = c.Complete(r.Lease, "OLD", 1000, base)
		}()
		go func() { // invalidator
			defer wg.Done()
			<-start
			_ = c.Invalidate("k", base)
		}()
		close(start) // 同步屏障：两者同时开始
		wg.Wait()

		// 竞态刚结束：无论谁先线性化，在途租约都应已结束（Complete 成功则
		// Invalidate 紧随其后清项；Invalidate 先则 Complete 被判 stale）。
		// 必须在下一次 Acquire 之前取快照，否则它本身会创建新 Leader。
		if st := c.Snapshot(); st.InFlight != 0 || st.Items != 0 {
			t.Fatalf("iter %d: right after race state=%+v (completeErr=%v), want empty",
				iter, st, completeErr)
		}

		// 关键读取不变量：此刻绝读不到 OLD。
		got, err := c.Acquire("k", base+1)
		if err != nil {
			t.Fatalf("post-race acquire: %v", err)
		}
		if got.Kind == KindHit && got.Value == "OLD" {
			t.Fatalf("iter %d: stale value OLD survived invalidate (completeErr=%v)", iter, completeErr)
		}
		if got.Kind == KindPending {
			t.Fatalf("iter %d: lease must not remain in flight after complete/invalidate race", iter)
		}
		// 空缓存下本次读取成为新 Leader；结束它以收尾。
		if got.Kind == KindLeader {
			if err := c.Fail(got.Lease, base+1); err != nil {
				t.Fatalf("iter %d: cleanup fail: %v", iter, err)
			}
		}
		if st := c.Snapshot(); st.InFlight != 0 {
			t.Fatalf("iter %d: inflight=%d, want 0", iter, st.InFlight)
		}
		_ = completeErr
	}
}

// Clear 竞态：多个不同键的在途租约 + Clear 屏障并发。Clear 线性化之后，
// 所有租约都必须 stale；Clear 之前完成的项若在 Clear 之后也必须不存在。
func TestConcurrentClearBarrier(t *testing.T) {
	const n = 8
	for iter := 0; iter < 100; iter++ {
		c := mustNew(t, n, n)
		leases := make([]*Lease, n)
		base := int64(iter * 10)
		for i := 0; i < n; i++ {
			r, err := c.Acquire(keyFor(i), base)
			if err != nil {
				t.Fatal(err)
			}
			leases[i] = r.Lease
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(n + 1)
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			i := i
			go func() {
				defer wg.Done()
				<-start
				errs[i] = c.Complete(leases[i], "v", 1000, base+1)
			}()
		}
		go func() {
			defer wg.Done()
			<-start
			_ = c.Clear(base + 1)
		}()
		close(start)
		wg.Wait()

		st := c.Snapshot()
		if st.Items != 0 || st.InFlight != 0 || st.Keys != 0 {
			t.Fatalf("iter %d: after Clear, state=%+v", iter, st)
		}
		// 所有旧句柄必须 stale，且重复 Complete 也是 stale。
		for i := 0; i < n; i++ {
			if err := c.Complete(leases[i], "x", 1, base+2); !errors.Is(err, ErrStaleLease) {
				t.Fatalf("iter %d key %d: post-clear complete err=%v, want stale", iter, i, err)
			}
		}
	}
}

// 高水位竞态：所有线程使用同一 tick，任何成功都合法；之后更小的 tick 必被拒。
func TestConcurrentSameTickHighWater(t *testing.T) {
	c := mustNew(t, 128, 128)
	const n = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Acquire(keyFor(i), 10)
		}()
	}
	close(start)
	wg.Wait()

	if st := c.Snapshot(); st.HighWater != 10 || st.InFlight != n {
		t.Fatalf("snapshot=%+v, want highwater=10 inflight=%d", st, n)
	}
	if _, err := c.Acquire("late", 9); !errors.Is(err, ErrRollback) {
		t.Fatalf("post-barrier lower tick err=%v, want rollback", err)
	}
}

func keyFor(i int) string {
	return string(rune('a'+i%26)) + "-" + itoa(i)
}

// itoa 避免在测试里引入 strconv 造成的命名噪音。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
