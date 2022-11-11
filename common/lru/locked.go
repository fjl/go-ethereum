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

import "sync"

// LockedCache is an LRU cache protected by a mutex.
type LockedCache[K comparable, V any] struct {
	cache Cache[K, V]
	mu    sync.Mutex
}

func NewLockedCache[K comparable, V any](capacity int) *LockedCache[K, V] {
	return &LockedCache[K, V]{cache: NewCache[K, V](capacity)}
}

// Add adds a value to the cache. Returns true if an item was evicted to store the new item.
func (c *LockedCache[K, V]) Add(key K, value V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Add(key, value)
}

// Contains reports whether the given key exists in the cache.
func (c *LockedCache[K, V]) Contains(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Contains(key)
}

// Get retrieves a value from the cache. This marks the key as recently used.
func (c *LockedCache[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Get(key)
}

// Len returns the current number of items in the cache.
func (c *LockedCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Len()
}

// Peek retrieves a value from the cache, but does not mark the key as recently used.
func (c *LockedCache[K, V]) Peek(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Peek(key)
}

// Purge empties the cache.
func (c *LockedCache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache.Purge()
}

// Remove drops an item from the cache. Returns true if the key was present in cache.
func (c *LockedCache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Remove(key)
}
