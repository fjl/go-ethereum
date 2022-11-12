// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package lru

import (
	"fmt"
	"io"
	"math/rand"
	"testing"
)

func TestCache(t *testing.T) {
	cache := NewBasicLRU[int, int](128)

	for i := 0; i < 256; i++ {
		cache.Add(i, i)
	}
	if cache.Len() != 128 {
		t.Fatalf("bad len: %v", cache.Len())
	}

	for i := 0; i < 128; i++ {
		_, ok := cache.Get(i)
		if ok {
			t.Fatalf("%d should be evicted", i)
		}
	}
	for i := 128; i < 256; i++ {
		_, ok := cache.Get(i)
		if !ok {
			t.Fatalf("%d should not be evicted", i)
		}
	}

	for i := 128; i < 192; i++ {
		ok := cache.Remove(i)
		if !ok {
			t.Fatalf("%d should be in cache", i)
		}
		ok = cache.Remove(i)
		if ok {
			t.Fatalf("%d should not be in cache", i)
		}
		_, ok = cache.Get(i)
		if ok {
			t.Fatalf("%d should be deleted", i)
		}
	}

	cache.Purge()
	if cache.Len() != 0 {
		t.Fatalf("bad len: %v", cache.Len())
	}
	if _, ok := cache.Get(200); ok {
		t.Fatalf("should contain nothing")
	}
}

func TestBasicLRUAddExistingKey(t *testing.T) {
	cache := NewBasicLRU[int, int](1)

	cache.Add(1, 1)
	cache.Add(1, 2)

	v, _ := cache.Get(1)
	if v != 2 {
		t.Fatal("wrong value:", v)
	}
}

// This test checks GetOldest and RemoveOldest.
func TestCacheGetOldest(t *testing.T) {
	cache := NewBasicLRU[int, int](128)
	for i := 0; i < 256; i++ {
		cache.Add(i, i)
	}

	k, _, ok := cache.GetOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != 128 {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = cache.RemoveOldest()
	if !ok {
		t.Fatalf("missing")
	}
	if k != 128 {
		t.Fatalf("bad: %v", k)
	}

	k, _, ok = cache.RemoveOldest()
	if !ok {
		t.Fatalf("missing oldest item")
	}
	if k != 129 {
		t.Fatalf("wrong oldest item: %v", k)
	}
}

// Test that Add returns true/false if an eviction occurred
func TestCacheAddReturnValue(t *testing.T) {
	cache := NewBasicLRU[int, int](1)
	if cache.Add(1, 1) {
		t.Errorf("first add shouldn't have evicted")
	}
	if !cache.Add(2, 2) {
		t.Errorf("second add should have evicted")
	}
}

// This test verifies that Contains doesn't change item recency.
func TestCacheContains(t *testing.T) {
	cache := NewBasicLRU[int, int](2)
	cache.Add(1, 1)
	cache.Add(2, 2)
	if !cache.Contains(1) {
		t.Errorf("1 should be in the cache")
	}
	cache.Add(3, 3)
	if cache.Contains(1) {
		t.Errorf("Contains should not have updated recency of 1")
	}
}

func BenchmarkLRU(b *testing.B) {
	var (
		capacity = 1000
		indexes  = make([]int, capacity*20)
		keys     = make([]string, capacity)
		values   = make([][]byte, capacity)
	)
	for i := range indexes {
		indexes[i] = rand.Intn(capacity)
	}
	for i := range keys {
		b := make([]byte, 32)
		rand.Read(b)
		keys[i] = string(b)
		rand.Read(b)
		values[i] = b
	}

	var sink []byte

	b.Run("Add/BasicLRU", func(b *testing.B) {
		cache := NewBasicLRU[int, int](capacity)
		for i := 0; i < b.N; i++ {
			cache.Add(i, i)
		}
	})
	b.Run("Get/BasicLRU", func(b *testing.B) {
		cache := NewBasicLRU[string, []byte](capacity)
		for i := 0; i < capacity; i++ {
			index := indexes[i]
			cache.Add(keys[index], values[index])
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keys[indexes[i%len(indexes)]]
			v, ok := cache.Get(k)
			if ok {
				sink = v
			}
		}
	})

	// // vs. github.com/hashicorp/golang-lru/simplelru
	// b.Run("Add/simplelru.LRU", func(b *testing.B) {
	// 	cache, _ := simplelru.NewLRU(capacity, nil)
	// 	for i := 0; i < b.N; i++ {
	// 		cache.Add(i, i)
	// 	}
	// })
	// b.Run("Get/simplelru.LRU", func(b *testing.B) {
	// 	cache, _ := simplelru.NewLRU(capacity, nil)
	// 	for i := 0; i < capacity; i++ {
	// 		index := indexes[i]
	// 		cache.Add(keys[index], values[index])
	// 	}
	//
	// 	b.ResetTimer()
	// 	for i := 0; i < b.N; i++ {
	// 		k := keys[indexes[i%len(indexes)]]
	// 		v, ok := cache.Get(k)
	// 		if ok {
	// 			sink = v.([]byte)
	// 		}
	// 	}
	// })

	fmt.Fprintln(io.Discard, sink)
}
