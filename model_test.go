package leasecache

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

// 本文件提供一个独立编写的顺序参考模型，用固定种子的小规模随机操作流
// 对真实实现做差分测试，逐操作比较返回结果与全部可观测状态。
//
// 参考模型刻意用最直白的方式重写规格，以交叉校验边界条件
// （过期判定、LRU 序号、分代失效、高水位、错误无副作用）。

type refEntry struct {
	value     string
	expiresAt int64
	lruSeq    uint64
}

type refKeyState struct {
	gen     uint64
	leaseID uint64
	hasItem bool
	item    refEntry
}

type refLeaseRec struct {
	key string
	gen uint64
}

type refModel struct {
	capacity    int
	maxInFlight int
	states      map[string]*refKeyState
	leases      map[uint64]*refLeaseRec
	inFlight    int
	nItems      int
	nextID      uint64
	nextLRU     uint64
	now         int64
	epoch       uint64
}

func newRefModel(capacity, maxInFlight int) *refModel {
	return &refModel{
		capacity:    capacity,
		maxInFlight: maxInFlight,
		states:      make(map[string]*refKeyState),
		leases:      make(map[uint64]*refLeaseRec),
	}
}

type refResult struct {
	kind    Kind
	value   string
	leaseID uint64
}

func (m *refModel) checkTick(now int64) error {
	if now < 0 {
		return ErrNegativeTick
	}
	if now < m.now {
		return ErrRollback
	}
	return nil
}

func (m *refModel) liveLease(id uint64, key string, gen uint64) (*refKeyState, bool) {
	if id == 0 {
		return nil, false
	}
	rec, ok := m.leases[id]
	if !ok || rec.key != key || rec.gen != gen {
		return nil, false
	}
	st := m.states[key]
	if st == nil || st.leaseID != id || st.gen != gen {
		return nil, false
	}
	return st, true
}

func (m *refModel) Acquire(key string, now int64) (refResult, error) {
	if err := m.checkTick(now); err != nil {
		return refResult{}, err
	}
	st := m.states[key]
	if st != nil && st.hasItem && now < st.item.expiresAt {
		m.nextLRU++
		st.item.lruSeq = m.nextLRU
		m.now = now
		return refResult{kind: KindHit, value: st.item.value}, nil
	}
	hasLease := st != nil && st.leaseID != 0
	if !hasLease {
		if m.inFlight >= m.maxInFlight {
			return refResult{}, ErrInFlightLimit
		}
		if m.nextID == math.MaxUint64 {
			return refResult{}, ErrLeaseIDsExhausted
		}
	}
	if st != nil && st.hasItem {
		st.hasItem = false
		st.item = refEntry{}
		m.nItems--
	}
	if hasLease {
		m.now = now
		return refResult{kind: KindPending, leaseID: st.leaseID}, nil
	}
	if st == nil {
		st = &refKeyState{}
		m.states[key] = st
	}
	if st.gen == 0 {
		st.gen = 1
	}
	m.nextID++
	m.leases[m.nextID] = &refLeaseRec{key: key, gen: st.gen}
	st.leaseID = m.nextID
	m.inFlight++
	m.now = now
	return refResult{kind: KindLeader, leaseID: m.nextID}, nil
}

func (m *refModel) Complete(id uint64, key string, gen uint64, value string, ttl, now int64) error {
	if err := m.checkTick(now); err != nil {
		return err
	}
	st, ok := m.liveLease(id, key, gen)
	if !ok {
		return ErrStaleLease
	}
	if ttl <= 0 || ttl > math.MaxInt64-now {
		return ErrInvalidTTL
	}
	delete(m.leases, id)
	st.leaseID = 0
	m.inFlight--
	for _, s := range m.states {
		if s.hasItem && now >= s.item.expiresAt {
			s.hasItem = false
			s.item = refEntry{}
			m.nItems--
		}
	}
	for m.nItems >= m.capacity {
		var victim string
		var seq uint64 = math.MaxUint64
		for k, s := range m.states {
			if s.hasItem && s.item.lruSeq < seq {
				victim = k
				seq = s.item.lruSeq
			}
		}
		vs := m.states[victim]
		vs.hasItem = false
		vs.item = refEntry{}
		m.nItems--
	}
	st.hasItem = true
	m.nextLRU++
	st.item = refEntry{value: value, expiresAt: now + ttl, lruSeq: m.nextLRU}
	m.nItems++
	m.now = now
	return nil
}

func (m *refModel) Fail(id uint64, key string, gen uint64, now int64) error {
	if err := m.checkTick(now); err != nil {
		return err
	}
	st, ok := m.liveLease(id, key, gen)
	if !ok {
		return ErrStaleLease
	}
	delete(m.leases, id)
	st.leaseID = 0
	m.inFlight--
	m.now = now
	return nil
}

func (m *refModel) Invalidate(key string, now int64) error {
	if err := m.checkTick(now); err != nil {
		return err
	}
	st := m.states[key]
	if st == nil {
		st = &refKeyState{}
		m.states[key] = st
	}
	if st.gen == 0 {
		st.gen = 1
	} else {
		st.gen++
	}
	if st.leaseID != 0 {
		delete(m.leases, st.leaseID)
		st.leaseID = 0
		m.inFlight--
	}
	if st.hasItem {
		st.hasItem = false
		st.item = refEntry{}
		m.nItems--
	}
	m.now = now
	return nil
}

func (m *refModel) Clear(now int64) error {
	if err := m.checkTick(now); err != nil {
		return err
	}
	m.states = make(map[string]*refKeyState)
	m.leases = make(map[uint64]*refLeaseRec)
	m.inFlight = 0
	m.nItems = 0
	m.epoch++
	m.now = now
	return nil
}

func (m *refModel) stats() Stats {
	return Stats{
		Items:     m.nItems,
		InFlight:  m.inFlight,
		Keys:      len(m.states),
		HighWater: m.now,
		Epoch:     m.epoch,
	}
}

// ---- 差分测试 ----

type issuedLease struct {
	id  uint64
	key string
	gen uint64
}

func errCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNegativeTick):
		return "negative"
	case errors.Is(err, ErrRollback):
		return "rollback"
	case errors.Is(err, ErrStaleLease):
		return "stale"
	case errors.Is(err, ErrInvalidTTL):
		return "ttl"
	case errors.Is(err, ErrInFlightLimit):
		return "inflight"
	case errors.Is(err, ErrLeaseIDsExhausted):
		return "exhausted"
	default:
		return "other:" + err.Error()
	}
}

func TestDifferentialAgainstReference(t *testing.T) {
	// 固定种子、少量小样本：5 个种子，每个 1200 个操作，键空间 4，
	// 容量与在途上限都取小值以频繁触发驱逐与上限。
	for _, seed := range []int64{1, 42, 20260919, 777, 31337} {
		rng := rand.New(rand.NewSource(seed))
		cap, mf := 1+rng.Intn(3), 1+rng.Intn(3)
		real := mustNew(t, cap, mf)
		ref := newRefModel(cap, mf)
		handles := map[uint64]*Lease{} // 真实库的租约句柄
		var log []issuedLease
		water := int64(0)

		pickTick := func() int64 {
			switch rng.Intn(20) {
			case 0: // 回退
				return water - 1 - int64(rng.Intn(3))
			case 1: // 负值
				return -1
			default: // 相等或小幅前进
				return water + int64(rng.Intn(3))
			}
		}
		pickKey := func() string {
			return string(rune('k' + rng.Intn(4)))
		}
		pickLease := func() (issuedLease, bool) {
			if len(log) == 0 {
				return issuedLease{}, false
			}
			if rng.Intn(10) < 6 {
				return log[len(log)-1], true // 偏向最新句柄
			}
			return log[rng.Intn(len(log))], true
		}

		for step := 0; step < 1200; step++ {
			now := pickTick()
			switch rng.Intn(100) {
			case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
				10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
				20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
				30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
				40, 41, 42, 43, 44: // 45% Acquire
				key := pickKey()
				rr, rerr := real.Acquire(key, now)
				fr, ferr := ref.Acquire(key, now)
				if errCode(rerr) != errCode(ferr) {
					t.Fatalf("seed %d step %d Acquire(%q,%d) err: real=%v ref=%v", seed, step, key, now, rerr, ferr)
				}
				if rerr == nil {
					if rr.Kind != fr.kind || rr.Value != fr.value || rr.LeaseID != fr.leaseID {
						t.Fatalf("seed %d step %d Acquire(%q,%d) result mismatch: real=%+v ref=%+v", seed, step, key, now, rr, fr)
					}
					if rr.Kind == KindLeader {
						log = append(log, issuedLease{id: rr.LeaseID, key: key, gen: rr.Lease.Generation()})
						handles[rr.LeaseID] = rr.Lease
					}
				}
			case 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
				55, 56, 57, 58, 59, 60, 61, 62, 63, 64: // 20% Complete
				iss, ok := pickLease()
				if !ok {
					continue
				}
				ttls := []int64{0, -1, 1, 2, 5, 10, 1000}
				ttl := ttls[rng.Intn(len(ttls))]
				if step%97 == 0 {
					ttl = math.MaxInt64 // 溢出探针
				}
				val := "v" + itoa(rng.Intn(6))
				rerr := real.Complete(handles[iss.id], val, ttl, now)
				ferr := ref.Complete(iss.id, iss.key, iss.gen, val, ttl, now)
				if errCode(rerr) != errCode(ferr) {
					t.Fatalf("seed %d step %d Complete(id=%d,%q,gen=%d,ttl=%d,now=%d) err: real=%v ref=%v",
						seed, step, iss.id, iss.key, iss.gen, ttl, now, rerr, ferr)
				}
			case 65, 66, 67, 68, 69, 70, 71, 72: // 8% Fail
				iss, ok := pickLease()
				if !ok {
					continue
				}
				rerr := real.Fail(handles[iss.id], now)
				ferr := ref.Fail(iss.id, iss.key, iss.gen, now)
				if errCode(rerr) != errCode(ferr) {
					t.Fatalf("seed %d step %d Fail(id=%d,now=%d) err: real=%v ref=%v",
						seed, step, iss.id, now, rerr, ferr)
				}
			case 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84: // 12% Invalidate
				key := pickKey()
				rerr := real.Invalidate(key, now)
				ferr := ref.Invalidate(key, now)
				if errCode(rerr) != errCode(ferr) {
					t.Fatalf("seed %d step %d Invalidate(%q,%d) err: real=%v ref=%v",
						seed, step, key, now, rerr, ferr)
				}
			case 85, 86, 87, 88, 89, 90, 91, 92, 93, 94: // 10% Clear
				rerr := real.Clear(now)
				ferr := ref.Clear(now)
				if errCode(rerr) != errCode(ferr) {
					t.Fatalf("seed %d step %d Clear(%d) err: real=%v ref=%v", seed, step, now, rerr, ferr)
				}
			default: // 5% 快照比较
				if real.Snapshot() != ref.stats() {
					t.Fatalf("seed %d step %d snapshot mismatch: real=%+v ref=%+v",
						seed, step, real.Snapshot(), ref.stats())
				}
			}

			// 每一步都比较全部可观测状态。
			if rs, fs := real.Snapshot(), ref.stats(); rs != fs {
				t.Fatalf("seed %d step %d post-op(%d) state mismatch: real=%+v ref=%+v water=%d",
					seed, step, now, rs, fs, water)
			}
			water = ref.now
		}

		// 结束前再做一次全键 Hit/Leader 视图的一致性抽查。
		for _, k := range []string{"k", "l", "m", "n"} {
			rr, rerr := real.Acquire(k, water+1)
			fr, ferr := ref.Acquire(k, water+1)
			if errCode(rerr) != errCode(ferr) || (rerr == nil && (rr.Kind != fr.kind || rr.Value != fr.value || rr.LeaseID != fr.leaseID)) {
				t.Fatalf("seed %d final acquire %q mismatch: real=(%+v,%v) ref=(%+v,%v)",
					seed, k, rr, rerr, fr, ferr)
			}
		}
	}
}
