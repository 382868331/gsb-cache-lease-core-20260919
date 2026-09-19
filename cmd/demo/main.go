// Demo of the cachelease library: one normal load path and one real
// failure (a stale Complete rejected after Invalidate). Simulated loader
// latency makes the whole run take about 8 seconds.
package main

import (
	"errors"
	"fmt"
	"time"

	cachelease "github.com/382868331/gsb-cache-lease-core-20260919/cachelease"
)

// simulateLoad pretends to fetch a value from a slow origin.
func simulateLoad(key string, d time.Duration) string {
	time.Sleep(d)
	return "value-of-" + key
}

func main() {
	cache, err := cachelease.New(4, 2)
	if err != nil {
		panic(err)
	}
	var now int64 // caller-supplied tick; advanced as the demo progresses

	fmt.Println("=== 1. Normal path: miss -> Leader -> load -> Complete -> Hit ===")
	res, err := cache.Acquire("user:1", now)
	must(err)
	fmt.Printf("Acquire(user:1, now=%d) -> %s (lease id=%d)\n", now, res.Kind, res.Lease.ID())

	v := simulateLoad("user:1", 2*time.Second) // slow origin fetch
	now += 2
	must(cache.Complete(res.Lease, v, 100, now))
	fmt.Printf("Complete(lease=%d, %q, ttl=100, now=%d) -> ok\n", res.Lease.ID(), v, now)

	now++
	hit, err := cache.Acquire("user:1", now)
	must(err)
	fmt.Printf("Acquire(user:1, now=%d) -> %s value=%q\n", now, hit.Kind, hit.Value)

	fmt.Println()
	fmt.Println("=== 2. Duplicate acquirers coalesce onto one lease (Pending) ===")
	now++
	r1, err := cache.Acquire("user:2", now)
	must(err)
	fmt.Printf("Acquire(user:2, now=%d) -> %s (lease id=%d)\n", now, r1.Kind, r1.Lease.ID())
	r2, err := cache.Acquire("user:2", now)
	must(err)
	fmt.Printf("Acquire(user:2, now=%d) -> %s (in-flight lease id=%d)\n", now, r2.Kind, r2.LeaseID)
	v2 := simulateLoad("user:2", 2*time.Second)
	now += 2
	must(cache.Complete(r1.Lease, v2, 100, now))
	fmt.Printf("Complete(lease=%d, %q, ttl=100, now=%d) -> ok\n", r1.Lease.ID(), v2, now)

	fmt.Println()
	fmt.Println("=== 3. Real failure: Invalidate races an in-flight load ===")
	now++
	r3, err := cache.Acquire("user:3", now)
	must(err)
	fmt.Printf("Acquire(user:3, now=%d) -> %s (lease id=%d)\n", now, r3.Kind, r3.Lease.ID())

	// The loader starts; while it is in flight the key is invalidated.
	loadDone := make(chan string)
	go func() { loadDone <- simulateLoad("user:3", 2*time.Second) }()
	time.Sleep(1 * time.Second) // invalidation lands mid-load
	now++
	must(cache.Invalidate("user:3", now))
	fmt.Printf("Invalidate(user:3, now=%d) -> ok (lease %d retired)\n", now, r3.Lease.ID())

	staleValue := <-loadDone // the old load finally returns
	now++
	err = cache.Complete(r3.Lease, staleValue, 100, now)
	fmt.Printf("Complete(stale lease=%d, %q, ttl=100, now=%d) -> err=%v\n", r3.Lease.ID(), staleValue, now, err)
	if !errors.Is(err, cachelease.ErrStale) {
		fmt.Println("UNEXPECTED: stale Complete was not rejected")
	} else {
		fmt.Println("OK: stale load result was rejected and did not refill the cache")
	}

	// The new generation is unaffected: a fresh load can proceed.
	r4, err := cache.Acquire("user:3", now)
	must(err)
	fmt.Printf("Acquire(user:3, now=%d) -> %s (new lease id=%d, old id %d not reused)\n",
		now, r4.Kind, r4.Lease.ID(), r3.Lease.ID())

	fmt.Println()
	fmt.Printf("final: Now()=%d Len()=%d Inflight()=%d snapshot=%v\n",
		cache.Now(), cache.Len(), cache.Inflight(), cache.Snapshot())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
