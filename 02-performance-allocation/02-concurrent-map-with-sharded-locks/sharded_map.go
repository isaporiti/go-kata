package sharded_map

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
)

const shardNumber = 32

type ShardedMap[K comparable, V any] struct {
	shards  []map[K]V
	mutexes []sync.RWMutex
}

func NewShardedMap[K comparable, V any](options ...ShardedMapOption[K, V]) (*ShardedMap[K, V], error) {
	sm := &ShardedMap[K, V]{
		shards:  make([]map[K]V, shardNumber),
		mutexes: make([]sync.RWMutex, shardNumber),
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

func (s *ShardedMap[K, V]) Get(k K) V {
	i := s.getIndex(k)

	mu := &s.mutexes[i]
	mu.RLock()
	defer mu.RUnlock()

	m := s.shards[i]
	return m[k]
}

func (s *ShardedMap[K, V]) Set(k K, v V) {
	i := s.getIndex(k)

	mu := &s.mutexes[i]
	mu.Lock()
	defer mu.Unlock()

	m := s.shards[i]
	m[k] = v
}

func (s *ShardedMap[K, V]) getIndex(k K) int {
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

	return int(hash % uint64(len(s.shards)))
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
