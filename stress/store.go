package main

import (
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
)

const maxKeyStoreShards = 64

type keyStore struct {
	shards  []keyStoreShard
	hasKeys atomic.Bool
}

type keyStoreShard struct {
	mu   sync.RWMutex
	ring []kvEntry
	size int
	next int
}

type kvEntry struct {
	key         string
	val         string
	verifyValue bool
}

func newKeyStore(capacity, workers int) *keyStore {
	if capacity <= 0 {
		capacity = 1
	}
	shardCount := workers
	if shardCount <= 0 {
		shardCount = 1
	}
	if shardCount > maxKeyStoreShards {
		shardCount = maxKeyStoreShards
	}
	if shardCount > capacity {
		shardCount = capacity
	}

	shards := make([]keyStoreShard, shardCount)
	baseCapacity := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		shardCapacity := baseCapacity
		if i < extra {
			shardCapacity++
		}
		shards[i].ring = make([]kvEntry, shardCapacity)
	}
	return &keyStore{shards: shards}
}

func (s *keyStore) Put(key, val string, verifyValue bool) {
	shard := &s.shards[keyShard(key)%uint64(len(s.shards))]
	shard.mu.Lock()
	shard.ring[shard.next] = kvEntry{key: key, val: val, verifyValue: verifyValue}
	if shard.size < len(shard.ring) {
		shard.size++
	}
	shard.next = (shard.next + 1) % len(shard.ring)
	shard.mu.Unlock()
	s.hasKeys.Store(true)
}

func (s *keyStore) HasKeys() bool {
	return s.hasKeys.Load()
}

func (s *keyStore) GetRandom() (key string, val string, verifyValue bool, ok bool) {
	start := mrand.IntN(len(s.shards))
	for offset := range len(s.shards) {
		shard := &s.shards[(start+offset)%len(s.shards)]
		shard.mu.RLock()
		if shard.size > 0 {
			e := shard.ring[mrand.IntN(shard.size)]
			shard.mu.RUnlock()
			return e.key, e.val, e.verifyValue, true
		}
		shard.mu.RUnlock()
	}
	return "", "", false, false
}

func keyShard(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := range len(key) {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash
}
