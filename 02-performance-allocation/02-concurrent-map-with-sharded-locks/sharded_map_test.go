package sharded_map

import (
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
}
