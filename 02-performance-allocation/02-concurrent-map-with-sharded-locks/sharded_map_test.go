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
