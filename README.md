# 分代缓存租约与过期状态机

一个字符串键值的内存缓存协调库，用「分代 + 在途租约」防止失效之后旧加载结果回填缓存。
仅依赖 Go 标准库，单把互斥锁实现可线性化并发，不调用加载器、不阻塞、不联网。

模块路径：`github.com/382868331/gsb-cache-lease-core-20260919`
核心代码在包根目录（`cache.go`），演示在 `cmd/demo`。

## 环境与运行

- Windows 原生 Go 1.26.5，仅标准库，离线可跑；无第三方依赖、外部服务或 Docker。
- 演示（约毫秒级完成，远在 8 秒以内；输出一个正常结果和两个实际计算出的失败）：

  ```
  go run ./cmd/demo
  ```

- 测试：

  ```
  go test ./... -count=1 -timeout=60s
  ```

- 竞态检测：`go test -race` 需要 cgo（C 编译器）。本机 `CGO_ENABLED=0` 且无
  gcc/clang，故按任务要求不为此安装外部编译器；竞态正确性改用同步屏障测试覆盖。

## 接口

### 构造

```go
func New(capacity, maxInFlight int) (*Cache, error)
```

`capacity`（缓存项容量）与 `maxInFlight`（在途租约上限）都必须为正整数，否则返回
`ErrInvalidConfig`。容量只计缓存项，不计在途租约。

### Acquire

```go
func (c *Cache) Acquire(key string, now int64) (Result, error)
```

返回三态之一（`Result.Kind`）：

- `KindHit`：存在未过期项，`Result.Value` 为值，并刷新该项 LRU 序号；
- `KindLeader`：未命中且当前分代无在途租约，调用方成为唯一加载者，拿到
  `Result.Lease` / `Result.LeaseID`；
- `KindPending`：当前分代已有在途租约，`Result.LeaseID` 指向它（在途上限满时仍可查）。

`now` 是调用方传入的非负 `int64` tick。成功的 Acquire（含三种结果）把全局高水位
推进到 `now`：`now < 高水位` 返回 `ErrRollback`（时间回退），相等允许；负值返回
`ErrNegativeTick`。缓存项在 `now >= expiresAt` 时判定为恰好过期并惰性剔除。
在途上限满时新 Leader 返回 `ErrInFlightLimit`；租约 ID 计数耗尽返回
`ErrLeaseIDsExhausted`。

### Complete / Fail

```go
func (c *Cache) Complete(lease *Lease, value string, ttl, now int64) error
func (c *Cache) Fail(lease *Lease, now int64) error
```

- `Complete` 以租约回填值：要求 `ttl > 0` 且 `now+ttl` 不溢出 int64（否则
  `ErrInvalidTTL`）。写入时**先**剔除所有已过期项，容量不足再按最小 LRU 序号驱逐，
  最后写入并刷新 LRU。
- `Fail` 结束租约但不写缓存；之后同键可再次 Acquire 成为 Leader（新 ID，不复用）。
- 租约只由 `Complete` / `Fail` / `Invalidate` / `Clear` 结束，不随时间自动到期。
- 失效分代中的旧句柄一律返回 `ErrStaleLease`，绝不影响新分代或缓存。

### Invalidate / Clear

```go
func (c *Cache) Invalidate(key string, now int64) error
func (c *Cache) Clear(now int64) error
```

`Invalidate` 清掉指定键的缓存项与其当前在途租约，并把该键分代 +1；`Clear` 清空全部
缓存项与全部租约并进入新的 Epoch。两者都是成功的修改操作，都会推进全局高水位；
它们之前线性化的旧租约在此之后全部 stale。

### 只读快照

```go
func (c *Cache) Snapshot() Stats // 不校验、不推进时钟
```

`Stats` 含 `Items`（缓存项数）、`InFlight`（在途租约数）、`Keys`、`HighWater`、`Epoch`。

### 错误集合

`ErrInvalidConfig`、`ErrNegativeTick`、`ErrRollback`、`ErrStaleLease`、
`ErrInvalidTTL`、`ErrInFlightLimit`、`ErrLeaseIDsExhausted`，用 `errors.Is` 判断。
**任何错误（stale、时间回退、超限、非法 ttl 等）都不改变数据、LRU、租约或时钟。**

## 语义要点

- 全局 tick 高水位：所有成功的修改与 Acquire 在线性化顺序上维护；只读快照不推进。
- 同键同代至多一个在途租约；租约 ID 全局单调、永不复用。
- LRU 以单调序号定序：成功 Hit 与 Complete 刷新序号；驱逐先扫过期项再按最旧序号淘汰。
- 并发：所有 API 持同一把 `sync.Mutex`，线性化点即持锁区，无需无锁算法。

## 测试组织

- `cache_test.go`：同 tick、时间回退、恰好过期边界、旧租约回填、租约不自动到期、
  LRU（容量 1、Hit 刷新、写入前先剔过期）、在途上限、非法/溢出 TTL、
  错误不推进时钟不改状态、快照不推进时钟、ID 不复用。
- `concurrent_test.go`：用 channel/WaitGroup **同步屏障**（无 sleep）构造
  Invalidate↔Complete 与 Clear↔多租约竞态，以及同 tick 高水位竞态。
- `model_test.go`：独立编写的顺序参考模型，固定 5 个种子 × 1200 个小操作
  （小键空间、小容量/在途上限，含回退、负值、零/负/溢出 ttl 探针），逐操作差分比较
  返回结果与完整快照。
