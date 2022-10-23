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
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
	// searchTableDepth is the number of buckets kept in the search table.
	//
	// The table only keeps nodes at logdist(topic, n) > (256 - searchTableDepth).
	// Should there be any nodes which are closer than this, they just go into the last
	// (closest) bucket.
	searchTableDepth = 40

	// IP subnet limit.
	searchBucketSubnet, searchBucketIPLimit = 24, 1
)

// Search is the state associated with searching for a single topic.
type Search struct {
	topic TopicID
	cfg   Config
	log   log.Logger

	// Note: search buckets are ordered far -> close.
	buckets [searchTableDepth]searchBucket

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	numResults   int

	queriesWithoutNewNodes int
}

type searchBucket struct {
	dist       int
	new        map[enode.ID]*enode.Node
	asked      map[enode.ID]struct{}
	numResults int

	ips netutil.DistinctNetSet
}

// NewSearch creates a new topic search state.
func NewSearch(topic TopicID, cfg Config) *Search {
	cfg = cfg.withDefaults()
	s := &Search{
		cfg:         cfg,
		log:         cfg.Log.New("topic", topic),
		topic:       topic,
		bucketCheck: make(map[int]struct{}, searchTableDepth),
	}
	dist := 256
	for i := range s.buckets {
		s.buckets[i] = searchBucket{
			dist:  dist,
			new:   make(map[enode.ID]*enode.Node, cfg.SearchBucketSize),
			asked: make(map[enode.ID]struct{}, cfg.SearchBucketSize),
			ips: netutil.DistinctNetSet{
				Subnet: searchBucketSubnet,
				Limit:  searchBucketIPLimit,
			},
		}
		dist--
	}
	return s
}

// IsDone reports whether the search table is saturated. When it returns true,
// this search state should be abandoned and a new search started using a
// fresh Search instance.
func (s *Search) IsDone() bool {
	// TODO: what's the condition here?
	//
	// Ideas:
	//
	//   - n total results reached
	//   - results from n sources received
	//   - closest nodes reached (requires improved lookup tracking)
	//   - buckets fuller than X

	// The search cannot be done while there are unused results in the buffer,
	// or while there are still nodes that could be asked.
	if len(s.resultBuffer) > 0 {
		return false
	}
	for _, b := range s.buckets {
		if len(b.new) > 0 {
			return false
		}
	}
	// No unasked nodes remain. Consider it done when the last
	// two lookups didn't yield any new nodes.
	return s.queriesWithoutNewNodes >= 2
}

// AddNodes adds potential registrars to the table.
// If src is non-nil, it is assumed that the nodes were sent by that node.
func (s *Search) AddNodes(src *enode.Node, nodes []*enode.Node) {
	// Clear the one-per-bucket check table.
	for k := range s.bucketCheck {
		delete(s.bucketCheck, k)
	}

	var anyNewNode bool
	for _, n := range nodes {
		id := n.ID()
		if id == s.cfg.Self {
			continue
		}

		bi := s.bucketIndex(n.ID())
		b := &s.buckets[bi]

		if b.contains(id) {
			continue
		}
		if b.count() >= s.cfg.SearchBucketSize {
			continue
		}
		if src != nil {
			if _, ok := s.bucketCheck[bi]; ok {
				s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "one-per-bucket-rule")
				continue
			}
		}
		if !b.ips.Add(n.IP()) {
			s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "iplimit")
		}

		// All checks passed, add the node.
		anyNewNode = true
		b.new[id] = n
	}

	if !anyNewNode {
		s.queriesWithoutNewNodes++
	} else {
		s.queriesWithoutNewNodes = 0
	}
}

// QueryTarget returns a random node to which a topic query should be sent.
func (s *Search) QueryTarget() *enode.Node {
	for _, b := range s.buckets {
		for _, n := range b.new {
			return n
		}
	}
	return nil
}

// AddQueryResults adds the response nodes for a topic query to the table.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	b := s.bucket(from.ID())
	b.setAsked(from)

	for _, n := range results {
		if n.ID() == s.cfg.Self {
			continue
		}
		s.cfg.Log.Debug("Added topic search result", "topic", s.topic, "fromid", from.ID(), "rid", n.ID())
		b.numResults++
		s.numResults++
		s.resultBuffer = append(s.resultBuffer, n)
	}
}

// PeekResult returns a node from the result set.
// When no result is available, it returns nil.
func (s *Search) PeekResult() *enode.Node {
	if len(s.resultBuffer) > 0 {
		return s.resultBuffer[0]
	}
	return nil
}

// PopResult removes a result node.
func (s *Search) PopResult() {
	if len(s.resultBuffer) == 0 {
		panic("PopResult with len(results) == 0")
	}
	s.resultBuffer = append(s.resultBuffer[:0], s.resultBuffer[1:]...)
}

func (s *Search) bucketIndex(id enode.ID) int {
	dist := 256 - enode.LogDist(enode.ID(s.topic), id)
	if dist > len(s.buckets)-1 {
		dist = len(s.buckets) - 1
	}
	return dist
}

func (s *Search) bucket(id enode.ID) *searchBucket {
	return &s.buckets[s.bucketIndex(id)]
}

func (b *searchBucket) contains(id enode.ID) bool {
	_, inNew := b.new[id]
	_, inAsked := b.asked[id]
	return inNew || inAsked
}

func (b *searchBucket) count() int {
	return len(b.new) + len(b.asked)
}

func (b *searchBucket) setAsked(n *enode.Node) {
	b.asked[n.ID()] = struct{}{}
	delete(b.new, n.ID())
}
