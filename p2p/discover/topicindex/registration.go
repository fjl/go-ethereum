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
	"container/heap"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const regBucketMaxNodes = 10

// Registration is the state associated with registering in a single topic.
type Registration struct {
	clock   mclock.Clock
	topic   TopicID
	buckets [257]regBucket
	regHeap regHeap
}

type regBucket struct {
	active   map[enode.ID]mclock.AbsTime
	attempts map[enode.ID]*RegAttempt
}

type RegAttempt struct {
	Node   *enode.Node
	Ticket []byte
	When   mclock.AbsTime

	index int // index in regHeap
}

func NewRegistration(topic TopicID, config Config) *Registration {
	config = config.withDefaults()
	r := &Registration{clock: config.Clock}
	for i := range r.buckets {
		r.buckets[i].active = make(map[enode.ID]mclock.AbsTime)
		r.buckets[i].attempts = make(map[enode.ID]*RegAttempt)
	}
	return r
}

// Topic gives the topic being registered for.
func (r *Registration) Topic() TopicID {
	return r.topic
}

// LookupTarget returns a target that should be walked towards.
func (r *Registration) LookupTarget() enode.ID {
	return enode.ID(r.topic)
}

// AddNodes notifies the registration process about found nodes.
func (r *Registration) AddNodes(nodes []*enode.Node) {
	for _, n := range nodes {
		b := r.bucket(n)
		if _, ok := b.active[n.ID()]; ok {
			continue // has registration with this node
		}
		if _, ok := b.attempts[n.ID()]; ok {
			continue // has scheduled attempt with this node
		}
		if len(b.attempts) > regBucketMaxNodes {
			continue // contains enough attempts already
		}

		attempt := &RegAttempt{Node: n, When: r.clock.Now()}
		b.attempts[n.ID()] = attempt
		heap.Push(&r.regHeap, attempt)
	}
}

// NextRequest returns a registration attempt that should be made.
// If there is nothing to do, this method returns nil.
func (r *Registration) NextRequest() *RegAttempt {
	if len(r.regHeap) == 0 {
		return nil
	}
	return r.regHeap[0]
}

// HandleTicketResponse should be called when a node responds to a registration
// request with a ticket and waiting time.
func (r *Registration) HandleTicketResponse(n *enode.Node, ticket []byte, waitTime time.Duration) {
	b := r.bucket(n)
	attempt := b.attempts[n.ID()]
	if attempt == nil {
		return
	}
	attempt.Ticket = ticket
	attempt.Node = n
	attempt.When = r.clock.Now().Add(waitTime)
	heap.Fix(&r.regHeap, attempt.index)
}

// HandleRegistered should be called when a node confirms topic registration.
func (r *Registration) HandleRegistered(n *enode.Node) {
	b := r.bucket(n)
	if attempt := b.attempts[n.ID()]; attempt != nil {
		delete(b.attempts, n.ID())
		heap.Remove(&r.regHeap, attempt.index)
	}
	b.active[n.ID()] = r.clock.Now()
}

func (r *Registration) bucket(n *enode.Node) *regBucket {
	return &r.buckets[enode.LogDist(enode.ID(r.topic), n.ID())]
}

// regHeap is a priority queue of registration attempts. This should not be accessed
// directly. Use heap.Push and heap.Pop to add and remove items.
type regHeap []*RegAttempt

func (rh regHeap) Len() int {
	return len(rh)
}

func (rh regHeap) Less(i, j int) bool {
	return rh[i].When < rh[j].When
}

func (rh regHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
	rh[i].index = i
	rh[j].index = j
}

func (rh *regHeap) Push(x interface{}) {
	n := len(*rh)
	item := x.(*RegAttempt)
	item.index = n
	*rh = append(*rh, item)
}

func (rh *regHeap) Pop() interface{} {
	old := *rh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*rh = old[0 : n-1]
	return item
}
