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

package topicindex

import (
	"testing"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// func TestSearchLookups(t *testing.T) {
// 	config := testConfig(t)
// 	s := NewSearch(topic1, config)
//
// 	t.Log(s.LookupTarget())
// }

// This checks that search buckets are filled correctly
// with nodes at various distances.
func TestSearchBuckets(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	var (
		far256  = nodesAtDistance(enode.ID(topic1), 256, 3)
		far255  = nodesAtDistance(enode.ID(topic1), 255, 3)
		close5  = nodesAtDistance(enode.ID(topic1), 5, 1)
		close20 = nodesAtDistance(enode.ID(topic1), 20, 1)
	)
	s.AddNodes(nil, far256)
	s.AddNodes(nil, far255)
	s.AddNodes(nil, close5)
	s.AddNodes(nil, close20)

	last := len(s.buckets) - 1
	if !sbContainsAll(s.buckets[0], far256) {
		t.Fatal("far256 nodes missing in bucket[0]")
	}
	if !sbContainsAll(s.buckets[1], far255) {
		t.Fatal("far255 nodes missing in bucket[1]")
	}
	if !sbContainsAll(s.buckets[last], close5) {
		t.Fatalf("close5 nodes missing in bucket[%d]", last)
	}
	if !sbContainsAll(s.buckets[last], close20) {
		t.Fatalf("close20 nodes missing in bucket[%d]", last)
	}
}

func sbContainsAll(b searchBucket, nodes []*enode.Node) bool {
	for _, n := range nodes {
		if !b.contains(n.ID()) {
			return false
		}
	}
	return true
}

// This checks (de)queueing of topic search results.
func TestSearchResultsTracking(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	var (
		src   = enode.SignNull(new(enr.Record), enode.ID{})
		nodes = nodesAtDistance(src.ID(), 256, 10)
	)
	s.AddQueryResults(src, nodes)

	for i, n := range nodes {
		result := s.PeekResult()
		if result.ID() != n.ID() {
			t.Logf("wrong result %d: got %v, want %v", i, result.ID(), n.ID())
		}
		s.PopResult()
	}
}
