// Package cachelease implements a generational cache-lease coordinator.
//
// The cache stores string key/value items with a fixed capacity, per-item
// expiry and generation information. Callers coordinate loads through
// leases: Acquire returns a Hit, a Leader lease (this caller loads), or
// Pending (another caller already holds the lease for the same key and
// generation). Invalidate and Clear retire cached items and outstanding
// leases; a stale Complete/Fail from an older generation is rejected and
// can never refill the cache.
//
// Time is a caller-supplied non-negative int64 tick. Every successful
// mutating call and every successful Acquire maintains a global high-water
// mark in linearization order: a now below the mark is rejected, an equal
// now is allowed. Any error (stale, clock regression, limits, invalid
// arguments) leaves data, LRU order, leases and the clock untouched.
//
// The cache never calls loaders, never blocks and never touches the
// network. All methods are safe for concurrent use and linearizable; a
// single mutex is used internally.
package cachelease

import (
	"container/list"
	"errors"
	"math"
	"sort"
	"sync"
)

// Sentinel errors. Match with errors.Is.
var (
	// ErrNegativeTime: now must be a non-negative tick.
	ErrNegativeTime = errors.New("cachelease: negative time")
	// ErrClockRegression: now is below the global high-water mark.
	ErrClockRegression = errors.New("cachelease: time before high-water mark")
	// ErrStale: the lease belongs to a retired generation or is unknown.
	ErrStale = errors.New("cachelease: stale lease")
	// ErrInflightLimit: the in-flight lease limit is reached; no new Leader.
	ErrInflightLimit = errors.New("cachelease: in-flight limit reached")
	// ErrLeaseIDExhausted: the lease ID counter is exhausted.
	ErrLeaseIDExhausted = errors.New("cachelease: lease ID space exhausted")
	// ErrInvalidTTL: ttl must be positive.
	ErrInvalidTTL = errors.New("cachelease: ttl must be positive")
	// ErrOverflow: now+ttl would overflow int64.
	ErrOverflow = errors.New("cachelease: now+ttl overflows int64")
	// ErrInvalidConfig: capacity and in-flight limit must be positive.
	ErrInvalidConfig = errors.New("cachelease: capacity and in-flight limit must be positive")
)

// Kind is the outcome of an Acquire call.
type Kind int

const (
	// Hit: the value is cached and fresh at the caller's now.
	Hit Kind = iota
	// Leader: no fresh value and no lease for this key/generation; the
	// caller owns the returned lease and must Complete or Fail it.
	Leader
	// Pending: another caller holds the lease for this key/generation;
	// the returned ID identifies that in-flight lease.
	Pending
)

func (k Kind) String() string {
	switch k {
	case Hit:
		return "Hit"
	case Leader:
		return "Leader"
	case Pending:
		return "Pending"
	default:
		return "Unknown"
	}
}

// Lease identifies one in-flight load for one key and generation. It is
// a value token: callers pass it back to Complete or Fail. Leases never
// expire on their own; they end only via Complete, Fail, Invalidate or
// Clear. Lease IDs are never reused.
type Lease struct {
	id    uint64
	key   string
	epoch uint64
	gen   uint64
}

// ID returns the unique, never-reused lease ID.
func (l Lease) ID() uint64 { return l.id }

// Key returns the key the lease loads.
func (l Lease) Key() string { return l.key }

// Result is the outcome of Acquire. Exactly one of the Kind-specific
// fields is meaningful: Value for Hit, Lease for Leader, LeaseID for
// Pending.
type Result struct {
	Kind    Kind
	Value   string
	Lease   Lease
	LeaseID uint64
}

// Entry is one cached item in a Snapshot.
type Entry struct {
	Key       string
	Value     string
	ExpiresAt int64
}

type item struct {
	value     string
	expiresAt int64
	elem      *list.Element // back-reference into lru, Value is the key
}

type leaseRef struct {
	epoch uint64
	gen   uint64
	key   string
}

// Cache is a generational lease-coordinating cache. The zero value is
// not usable; construct with New.
type Cache struct {
	mu            sync.Mutex
	capacity      int
	inflightLimit int

	now     int64 // global high-water mark
	entries map[string]*item
	lru     *list.List // front = most recently used, Value is key string

	leases      map[uint64]Lease
	byKey       map[leaseRef]uint64
	nextLeaseID uint64 // 1..MaxUint64; 0 means exhausted

	epoch uint64            // bumped by Clear
	gen   map[string]uint64 // bumped by Invalidate
}

// New returns a Cache with the given item capacity and in-flight lease
// limit. Both must be positive integers.
func New(capacity, inflightLimit int) (*Cache, error) {
	if capacity <= 0 || inflightLimit <= 0 {
		return nil, ErrInvalidConfig
	}
	c := &Cache{
		capacity:      capacity,
		inflightLimit: inflightLimit,
		entries:       make(map[string]*item),
		lru:           list.New(),
		leases:        make(map[uint64]Lease),
		byKey:         make(map[leaseRef]uint64),
		nextLeaseID:   1,
		gen:           make(map[string]uint64),
	}
	return c, nil
}

// Acquire looks up key at time now. It returns Hit with the cached value
// when a fresh item exists, Pending with the in-flight lease ID when
// another caller already holds the lease for this key and generation, or
// Leader with a new lease otherwise. A successful Acquire (any kind)
// advances the high-water mark; an error changes nothing.
func (c *Cache) Acquire(key string, now int64) (Result, error) {
	if now < 0 {
		return Result{}, ErrNegativeTime
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < c.now {
		return Result{}, ErrClockRegression
	}

	if it, ok := c.entries[key]; ok {
		if now < it.expiresAt {
			c.lru.MoveToFront(it.elem)
			c.now = now
			return Result{Kind: Hit, Value: it.value}, nil
		}
		c.removeItem(key) // expired at now: now >= expiresAt
	}

	ref := leaseRef{epoch: c.epoch, gen: c.gen[key], key: key}
	if id, ok := c.byKey[ref]; ok {
		c.now = now
		return Result{Kind: Pending, LeaseID: id}, nil
	}

	if len(c.leases) >= c.inflightLimit {
		return Result{}, ErrInflightLimit
	}
	if c.nextLeaseID == 0 {
		return Result{}, ErrLeaseIDExhausted
	}
	id := c.nextLeaseID
	if id == math.MaxUint64 {
		c.nextLeaseID = 0 // exhausted after issuing this ID
	} else {
		c.nextLeaseID = id + 1
	}
	lease := Lease{id: id, key: key, epoch: c.epoch, gen: c.gen[key]}
	c.leases[id] = lease
	c.byKey[ref] = id
	c.now = now
	return Result{Kind: Leader, Lease: lease}, nil
}

// Complete stores value for the lease's key with the given ttl, ending
// the lease. ttl must be positive and now+ttl must not overflow; the item
// is expired once now >= expiresAt. Expired items are purged before LRU
// eviction is applied to enforce the capacity. A stale lease (retired by
// Invalidate/Clear or already ended) is rejected with ErrStale and
// changes nothing.
func (c *Cache) Complete(lease Lease, value string, ttl int64, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if ttl > math.MaxInt64-now {
		return ErrOverflow
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < c.now {
		return ErrClockRegression
	}
	if err := c.checkLease(lease); err != nil {
		return err
	}
	c.removeLease(lease)

	c.purgeExpired(now)
	expiresAt := now + ttl
	if it, ok := c.entries[lease.key]; ok {
		it.value = value
		it.expiresAt = expiresAt
		c.lru.MoveToFront(it.elem)
	} else {
		elem := c.lru.PushFront(lease.key)
		c.entries[lease.key] = &item{value: value, expiresAt: expiresAt, elem: elem}
	}
	for len(c.entries) > c.capacity {
		c.evictOldest()
	}
	c.now = now
	return nil
}

// Fail ends the lease without storing a value. A stale lease is rejected
// with ErrStale and changes nothing.
func (c *Cache) Fail(lease Lease, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < c.now {
		return ErrClockRegression
	}
	if err := c.checkLease(lease); err != nil {
		return err
	}
	c.removeLease(lease)
	c.now = now
	return nil
}

// Invalidate retires the cached item and any in-flight lease for key,
// moving the key to a new generation. It succeeds (and advances the
// high-water mark) even when nothing was cached or in flight.
func (c *Cache) Invalidate(key string, now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < c.now {
		return ErrClockRegression
	}
	c.removeItem(key)
	ref := leaseRef{epoch: c.epoch, gen: c.gen[key], key: key}
	if id, ok := c.byKey[ref]; ok {
		delete(c.byKey, ref)
		delete(c.leases, id)
	}
	c.gen[key]++
	c.now = now
	return nil
}

// Clear retires every cached item and every in-flight lease, moving all
// keys to a new generation.
func (c *Cache) Clear(now int64) error {
	if now < 0 {
		return ErrNegativeTime
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < c.now {
		return ErrClockRegression
	}
	c.entries = make(map[string]*item)
	c.lru.Init()
	c.leases = make(map[uint64]Lease)
	c.byKey = make(map[leaseRef]uint64)
	c.epoch++
	c.now = now
	return nil
}

// Now returns the current high-water mark. It is read-only and does not
// advance the clock.
func (c *Cache) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Len returns the number of cached items (including any not yet purged
// expired ones). Read-only.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Inflight returns the number of live leases. Read-only.
func (c *Cache) Inflight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.leases)
}

// Snapshot returns all stored items with their expiry, sorted by key.
// It is read-only and does not advance the clock.
func (c *Cache) Snapshot() []Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Entry, 0, len(c.entries))
	for k, it := range c.entries {
		out = append(out, Entry{Key: k, Value: it.value, ExpiresAt: it.expiresAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// checkLease reports ErrStale unless the lease is live and current.
// Caller must hold c.mu.
func (c *Cache) checkLease(lease Lease) error {
	live, ok := c.leases[lease.id]
	if !ok || live != lease {
		return ErrStale
	}
	return nil
}

// removeLease deletes a live lease from both indexes. Caller must hold c.mu.
func (c *Cache) removeLease(lease Lease) {
	delete(c.leases, lease.id)
	delete(c.byKey, leaseRef{epoch: lease.epoch, gen: lease.gen, key: lease.key})
}

// removeItem deletes key from the cache and the LRU list. Caller must hold c.mu.
func (c *Cache) removeItem(key string) {
	if it, ok := c.entries[key]; ok {
		c.lru.Remove(it.elem)
		delete(c.entries, key)
	}
}

// purgeExpired deletes every item with expiresAt <= now. Caller must hold c.mu.
func (c *Cache) purgeExpired(now int64) {
	for key, it := range c.entries {
		if now >= it.expiresAt {
			c.lru.Remove(it.elem)
			delete(c.entries, key)
		}
	}
}

// evictOldest evicts the least recently used item. Caller must hold c.mu.
func (c *Cache) evictOldest() {
	back := c.lru.Back()
	if back == nil {
		return
	}
	key := back.Value.(string)
	c.lru.Remove(back)
	delete(c.entries, key)
}
