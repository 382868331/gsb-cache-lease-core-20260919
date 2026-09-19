# 分代缓存租约与过期状态机

字符串键值内存缓存协调库（Go 1.26.5，仅标准库，Windows 原生离线可运行）。
缓存项有容量、过期时间和分代信息；调用方通过租约协调加载，失效（Invalidate/Clear）
之后在途旧加载的结果无法回填缓存。

## 接口

包 `cachelease`（`cachelease/` 目录），核心类型 `*Cache`：

```go
c, err := cachelease.New(capacity, inflightLimit) // 均为正整数
```

- `Acquire(key, now) (Result, error)` — 返回三种结果之一：
  - `Hit(value)`：缓存命中且新鲜（`now < expiresAt`）；
  - `Leader(lease)`：未命中且无在途租约，调用方持有租约负责加载；
  - `Pending(leaseID)`：同键同代已有他方租约在途（同键同代仅一个租约）。
- `Complete(lease, value, ttl, now) error` — 写入 `value`，`expiresAt = now+ttl`；
  要求 `ttl > 0` 且 `now+ttl` 不溢出 int64。`now >= expiresAt` 即视为过期。
- `Fail(lease, now) error` — 结束租约但不写入。
- `Invalidate(key, now) error` / `Clear(now) error` — 使相应缓存项与在途租约失效，
  进入新一代；旧租约的 `Complete`/`Fail` 返回 `ErrStale`，不影响新代。
- 只读方法 `Now()` / `Len()` / `Inflight()` / `Snapshot()`，不推进时钟。

语义要点：

- 时间是调用者传入的非负 int64 tick；所有成功的修改和 Acquire 维护全局高水位，
  `now` 小于高水位返回 `ErrClockRegression`，相等允许。
- 租约不会自动到期，只能由 Complete/Fail/Invalidate/Clear 结束；租约 ID 单调分配、
  不复用，uint64 计数耗尽返回 `ErrLeaseIDExhausted`。
- 容量只计缓存项；成功的 Hit/Complete 更新 LRU；写入时先剔除过期项再按 LRU 驱逐。
- 达到在途上限时新 Leader 返回 `ErrInflightLimit`，现有 Pending 仍可查询。
- 任何错误（stale/时间回退/超限/参数非法）都不改变数据、LRU、租约或时钟。
- 全部方法并发安全、可线性化（内部一把互斥锁）；库不调用加载器、不阻塞、不联网。

## 运行

```sh
go run ./cmd/demo                          # 约 8 秒演示：正常路径 + 真实触发的 stale 失败
go test ./... -count=1 -timeout=60s        # 全部测试
```

测试覆盖：同 tick、时间回退、恰好过期、旧租约回填、Clear 竞态（同步屏障 +
可线性化结果校验）、LRU 容量 1、在途上限、错误不推进时钟、租约 ID 耗尽、
并发屏障随机操作、固定种子顺序参考模型对比。

`-race` 需要 cgo 与 gcc；本环境（CGO_ENABLED=0、无 gcc）不可用，未安装外部编译器，
并发正确性由互斥锁与屏障测试保证。
