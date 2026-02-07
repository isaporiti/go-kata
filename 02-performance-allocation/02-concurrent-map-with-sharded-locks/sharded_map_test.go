package main

import (
	"errors"
	"fmt"
	"hash/fnv"
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
const SHARD_NUMBER = 32

type userId = int

type user = string

type ShardedMap[K comparable, V any] struct {
	shards  []map[K]V
	mutexes []sync.RWMutex
}

func (s *ShardedMap[K, V]) Set(k K, v V) {
	var hash uint64
	switch k := any(k).(type) {
	case string:
		fnv := fnv.New64a()
		fnv.Write([]byte(k))
		hash = fnv.Sum64()

	case int:
		hash = uint64(k)

	default:
		panic("unreachable")
	}

	i := int(hash % uint64(len(s.shards)))

	mu := &s.mutexes[i]
	mu.Lock()
	defer mu.Unlock()

	m := s.shards[i]
	m[k] = v
}

func (s *ShardedMap[K, V]) Keys() []K {
	var keys []K
	for i := range s.mutexes {
		s.mutexes[i].RLock()
		m := s.shards[i]
		for k := range m {
			keys = append(keys, k)
		}
		s.mutexes[i].RUnlock()
	}
	return keys
}

func NewShardedMap[K comparable, V any](options ...ShardedMapOption[K, V]) (*ShardedMap[K, V], error) {
	sm := &ShardedMap[K, V]{
		shards:  make([]map[K]V, SHARD_NUMBER),
		mutexes: make([]sync.RWMutex, SHARD_NUMBER),
	}
	for i := range sm.shards {
		sm.shards[i] = make(map[K]V)
	}

	for _, opt := range options {
		if err := opt(sm); err != nil {
			return nil, fmt.Errorf("can't create ShardedMap: %v", err)
		}
	}
	return sm, nil
}

type ShardedMapOption[K comparable, V any] func(sm *ShardedMap[K, V]) error

func WithNumberOfShards[K comparable, V any](n int8) ShardedMapOption[K, V] {
	return func(sm *ShardedMap[K, V]) error {
		if n <= 0 {
			return errors.New("number of shards must be greater than zero")
		}
		if n == int8(len(sm.shards)) {
			return nil
		}

		sm.shards = make([]map[K]V, n)
		for i := range sm.shards {
			sm.shards[i] = make(map[K]V)
		}

		sm.mutexes = make([]sync.RWMutex, n)
		return nil
	}
}

func TestShardedMapCreation(t *testing.T) {
	if _, err := NewShardedMap[userId, user](); err != nil {
		t.Fatal(err)
	}
	if _, err := NewShardedMap(WithNumberOfShards[userId, user](16)); err != nil {
		t.Fatal(err)
	}
}

func TestShardedMapKeys(t *testing.T) {
	sm, _ := NewShardedMap[userId, user]()

	var keys []userId = sm.Keys()
	if got := len(keys); got != 0 {
		t.Fatalf("want 0 keys, got %d", got)
	}
}

func TestShardedMapSet(t *testing.T) {
	sm, _ := NewShardedMap[userId, user]()

	sm.Set(1, "Alice")
	sm.Set(2, "Bob")

	keys := sm.Keys()
	if got := len(keys); got != 2 {
		t.Fatalf("want 2 keys, got %d", got)
	}
}
