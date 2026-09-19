// 演示程序：用真实计算展示缓存租约库的一个正常结果与一个实际触发的失败。
// 运行：go run ./cmd/demo
package main

import (
	"errors"
	"fmt"
	"time"

	lc "github.com/382868331/gsb-cache-lease-core-20260919"
)

func main() {
	start := time.Now()
	fmt.Println("== cache-lease-core 演示 ==")

	// ---------- 正常结果：Leader 回填 -> Hit，含 LRU/过期行为 ----------
	c, err := lc.New(2, 2) // 容量 2，在途上限 2
	if err != nil {
		panic(err)
	}

	r, err := c.Acquire("user:1", 100)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[正常] Acquire 未命中 -> %s, leaseID=%d, gen=%d\n",
		kindName(r.Kind), r.LeaseID, r.Lease.Generation())

	p, err := c.Acquire("user:1", 100) // 同 tick，允许相等
	if err != nil {
		panic(err)
	}
	fmt.Printf("[正常] 同键同代再取 -> %s(leaseID=%d)，只有一个在途加载\n",
		kindName(p.Kind), p.LeaseID)

	if err := c.Complete(r.Lease, "Alice", 50, 100); err != nil { // expiresAt=150
		panic(err)
	}
	h, err := c.Acquire("user:1", 120)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[正常] Complete 后读取 -> %s(value=%q)，ttl 内有效\n", kindName(h.Kind), h.Value)

	// 恰好过期边界：now==expiresAt(150) 即过期。
	m, err := c.Acquire("user:1", 150)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[正常] now==expiresAt=150 -> %s，过期项被惰性剔除\n", kindName(m.Kind))
	if err := c.Complete(m.Lease, "Alice-v2", 100, 150); err != nil {
		panic(err)
	}

	// LRU：容量 2，再写两个键，最早未用者被驱逐。
	for _, kv := range [][2]string{{"user:2", "Bob"}, {"user:3", "Carol"}} {
		rr, err := c.Acquire(kv[0], 160)
		if err != nil {
			panic(err)
		}
		if err := c.Complete(rr.Lease, kv[1], 1000, 160); err != nil {
			panic(err)
		}
	}
	probe1, _ := c.Acquire("user:1", 161) // 已驱逐：成为新 Leader
	_ = c.Fail(probe1.Lease, 161)         // 立即结束探测租约，保持输出快照干净
	probe2, _ := c.Acquire("user:2", 162) // 仍在缓存：Hit
	fmt.Printf("[正常] 容量=2：user:1 状态=%s（已按 LRU 驱逐），user:2 状态=%s\n",
		kindName(probe1.Kind), kindName(probe2.Kind))
	fmt.Printf("[正常] 快照：%+v\n", c.Snapshot())

	// ---------- 实际触发的失败：旧租约在失效后回填，必须 stale ----------
	c2, err := lc.New(4, 4)
	if err != nil {
		panic(err)
	}
	old, err := c2.Acquire("config", 0)
	if err != nil {
		panic(err)
	}
	// 加载在途期间，另一路操作使该键失效（例如配置被更新/清空）。
	if err := c2.Invalidate("config", 1); err != nil {
		panic(err)
	}
	// 旧加载终于返回并尝试回填 —— 库实际计算后拒绝。
	err = c2.Complete(old.Lease, "OUTDATED", 1000, 2)
	fmt.Printf("[失败] 旧租约在 Invalidate 后回填 -> 错误: %v\n", err)
	if !errors.Is(err, lc.ErrStaleLease) {
		panic(fmt.Sprintf("期望 ErrStaleLease，实际 %v", err))
	}

	// 新分代正常加载，旧值绝不会渗入。
	fresh, err := c2.Acquire("config", 2)
	if err != nil {
		panic(err)
	}
	if err := c2.Complete(fresh.Lease, "CURRENT", 1000, 2); err != nil {
		panic(err)
	}
	got, _ := c2.Acquire("config", 3)
	fmt.Printf("[失败] 新分代值=%q（旧值 %q 未回填），gen: %d -> %d\n",
		got.Value, "OUTDATED", old.Lease.Generation(), fresh.Lease.Generation())

	// 再触发一个时钟类失败：时间回退被拒且无副作用（当前高水位为 3）。
	if _, err := c2.Acquire("tick-test", 2); !errors.Is(err, lc.ErrRollback) {
		panic(fmt.Sprintf("期望 ErrRollback，实际 %v", err))
	}
	fmt.Printf("[失败] tick=2 低于高水位=3 -> 错误: %v（数据与时钟不变）\n", lc.ErrRollback)
	fmt.Printf("[失败] 快照：%+v\n", c2.Snapshot())

	fmt.Printf("== 演示完成，耗时 %v（要求约 8 秒内）==\n", time.Since(start).Round(time.Millisecond))
}

func kindName(k lc.Kind) string {
	switch k {
	case lc.KindHit:
		return "Hit"
	case lc.KindLeader:
		return "Leader"
	case lc.KindPending:
		return "Pending"
	default:
		return "Unknown"
	}
}
