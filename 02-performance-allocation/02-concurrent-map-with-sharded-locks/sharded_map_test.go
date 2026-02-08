package sharded_map

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// ## 🛠 The Challenge
// Implement `ShardedMap[K comparable, V any]` with configurable shard count that provides safe concurrent access.

// ### 1. Functional Requirements
// * [ ] Type-safe generic implementation (Go 1.18+)
// * [ ] `Get(key K) (V, bool)` - returns value and existence flag
// * [ ] `Set(key K, value V)` - inserts or updates
// * [ ] `Delete(key K)` - removes key
// * [ ] `Keys() []K` - returns all keys (order doesn't matter)
// * [x] Configurable number of shards at construction

type userId = int

type user = string

func TestShardedMapCreation(t *testing.T) {
	t.Parallel()
	if _, err := NewShardedMap[userId, user](); err != nil {
		t.Fatal(err)
	}
	if _, err := NewShardedMap(WithNumberOfShards[userId, user](16)); err != nil {
		t.Fatal(err)
	}
}

func TestShardedMapKeys(t *testing.T) {
	t.Parallel()
	sm, err := NewShardedMap[userId, user]()
	if err != nil {
		t.Fatal(err)
	}

	var keys []userId = sm.Keys()
	if got := len(keys); got != 0 {
		t.Fatalf("want 0 keys, got %d", got)
	}
}

func TestShardedMapSet(t *testing.T) {
	t.Parallel()

	// keys type == int
	{
		sm, err := NewShardedMap[userId, user]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set(1, "Alice")
		sm.Set(2, "Bob")

		keys := sm.Keys()
		if got := len(keys); got != 2 {
			t.Fatalf("want 2 keys, got %d", got)
		}

		sm.Set(2, "Foo")
		if got := sm.Get(2); got != "Foo" {
			t.Fatalf(`want "Foo", got %q`, got)

		}
	}

	// keys type == string
	{
		sm, err := NewShardedMap[user, userId]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set("Alice", 1)
		sm.Set("Bob", 2)

		keys := sm.Keys()
		if got := len(keys); got != 2 {
			t.Fatalf("want 2 keys, got %d", got)
		}

		sm.Set("Alice", 3)
		if got := sm.Get("Alice"); got != 3 {
			t.Fatalf("want 3, got %d", got)
		}
	}
}

func TestShardedMapGet(t *testing.T) {
	t.Parallel()

	// keys type == int
	{
		sm, err := NewShardedMap[userId, user]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set(1, "Alice")
		sm.Set(2, "Bob")

		if got := sm.Get(1); got != "Alice" {
			t.Fatalf(`want "Alice", got %q`, got)
		}
		if got := sm.Get(2); got != "Bob" {
			t.Fatalf(`want "Bob", got %q`, got)
		}
	}

	// keys type == string
	{
		sm, err := NewShardedMap[user, userId]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set("Alice", 1)
		sm.Set("Bob", 2)

		if got := sm.Get("Alice"); got != 1 {
			t.Fatalf(`want 1, got %d`, got)
		}
		if got := sm.Get("Bob"); got != 2 {
			t.Fatalf(`want 2, got %d`, got)
		}
	}
}

func TestShardedMapDelete(t *testing.T) {
	t.Parallel()

	// keys type == int
	{
		sm, err := NewShardedMap[userId, user]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set(1, "Alice")
		sm.Set(2, "Bob")
		sm.Delete(3)

		if got := len(sm.Keys()); got != 2 {
			t.Fatalf("want 2 keys, got %d", got)
		}

		sm.Delete(1)

		if got := len(sm.Keys()); got != 1 {
			t.Fatalf("want 1 key, got %d", got)
		}
		if got := sm.Get(1); got == "Alice" {
			t.Fatal(`"1: Alice" did not get deleted`)
		}
		if got := sm.Get(2); got != "Bob" {
			t.Fatal(`"2: Bob" is missing`)
		}
	}

	// keys type == string
	{
		sm, err := NewShardedMap[user, userId]()
		if err != nil {
			t.Fatal(err)
		}

		sm.Set("Alice", 1)
		sm.Set("Bob", 2)
		sm.Delete("Foo")

		if got := len(sm.Keys()); got != 2 {
			t.Fatalf("want 2 keys, got %d", got)
		}

		sm.Delete("Alice")

		if got := len(sm.Keys()); got != 1 {
			t.Fatalf("want 1 key, got %d", got)
		}
		if got := sm.Get("Alice"); got == 1 {
			t.Fatal(`"Alice: 1" did not get deleted`)
		}
		if got := sm.Get("Bob"); got != 2 {
			t.Fatal(`"Bob: 2" is missing`)
		}
	}
}

func benchmarkSet(b *testing.B, shards int8) {
	sm, err := NewShardedMap(
		WithNumberOfShards[int, string](shards),
	)
	if err != nil {
		b.Fatalf("failed to create sharded map: %v", err)
	}

	const numGoroutines = 8
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := range numGoroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := i; j < b.N; j += numGoroutines {
				sm.Set(j, strconv.Itoa(j))
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkSet_1Shard(b *testing.B) {
	benchmarkSet(b, 1)
}

func BenchmarkSet_64Shards(b *testing.B) {
	benchmarkSet(b, 64)
}

func TestShardedMap_Race(t *testing.T) {
	sm, err := NewShardedMap[int, int]()
	if err != nil {
		t.Fatalf("failed to create sharded map: %v", err)
	}

	const (
		numGoroutines = 8
		numOps        = 10_000
	)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(g int) {
			defer wg.Done()
			for i := range numOps {
				key := (g * numOps) + i

				switch i % 3 {
				case 0:
					sm.Set(key, key)
				case 1:
					sm.Get(key)
				case 2:
					sm.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestMemory_Overhead(t *testing.T) {
	const (
		n          = 1_000_000
		maxExtraMB = 50.0
	)

	measure := func(fillMap func()) uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		before := m.Alloc

		fillMap()

		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.Alloc - before
	}

	baseUsed := measure(func() {
		m := make(map[int]any, n)
		for i := range n {
			m[i] = i
		}
	})

	shardedUsed := measure(func() {
		sm, _ := NewShardedMap[int, any]()
		for i := range n {
			sm.Set(i, i)
		}
	})

	diffBytes := int64(shardedUsed) - int64(baseUsed)
	diffMB := float64(diffBytes) / 1024 / 1024

	if diffMB > maxExtraMB {
		t.Fatalf("memory overhead too high: %.2f MB > %.2f MB", diffMB, maxExtraMB)
	}
}
