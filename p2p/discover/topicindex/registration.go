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
	"fmt"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const (
	regBucketMaxAttempts     = 10
	regBucketMaxReplacements = 20
)

// Registration is the state associated with registering in a single topic.
type Registration struct {
	clock   mclock.Clock
	topic   TopicID
	buckets [40]regBucket
	heap    regHeap

	timeout time.Duration
}

// RegAttemptState is the state of a registration attempt on a node.
type RegAttemptState int

const (
	Standby RegAttemptState = iota
	Waiting
	Registered

	nRegStates = int(Registered) + 1
)

type regBucket struct {
	att   map[enode.ID]*RegAttempt
	count [nRegStates]int
}

type RegAttempt struct {
	State    RegAttemptState
	NextTime mclock.AbsTime

	Node          *enode.Node
	Ticket        []byte
	TotalWaitTime time.Duration

	index  int // index in regHeap
	bucket *regBucket
}

func NewRegistration(topic TopicID, config Config) *Registration {
	config = config.withDefaults()
	r := &Registration{
		clock:   config.Clock,
		timeout: config.RegAttemptTimeout,
	}
	for i := range r.buckets {
		r.buckets[i].att = make(map[enode.ID]*RegAttempt)
	}
	return r
}

// Topic returns the topic being registered for.
func (r *Registration) Topic() TopicID {
	return r.topic
}

// LookupTarget returns a target that should be walked towards.
func (r *Registration) LookupTarget() enode.ID {
	// This finds the closest bucket with no registrations.
	center := enode.ID(r.topic)
	for i := range r.buckets {
		if r.buckets[i].count[Registered] == 0 {
			dist := i + 256
			return idAtDistance(center, dist)
		}
	}
	return center
}

// idAtDistance returns a random hash such that enode.LogDist(a, b) == n
func idAtDistance(a enode.ID, n int) (b enode.ID) {
	if n == 0 {
		return a
	}
	// flip bit at position n, fill the rest with random bits
	b = a
	pos := len(a) - n/8 - 1
	bit := byte(0x01) << (byte(n%8) - 1)
	if bit == 0 {
		pos++
		bit = 0x80
	}
	b[pos] = a[pos]&^bit | ^a[pos]&bit // TODO: randomize end bits
	for i := pos + 1; i < len(a); i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}

// AddNodes notifies the registration process about found nodes.
func (r *Registration) AddNodes(nodes []*enode.Node) {
	for _, n := range nodes {
		id := n.ID()
		b := r.bucket(id)
		attempt, ok := b.att[id]
		if ok {
			// There is already an attempt scheduled with this node.
			// Update the record if newer.
			if attempt.Node.Seq() < n.Seq() {
				attempt.Node = n
			}
			continue
		}

		if b.count[Standby] >= regBucketMaxReplacements {
			// There are enough replacements already, ignore the node.
			continue
		}

		// Create a new attempt.
		att := &RegAttempt{Node: n, bucket: b}
		b.att[id] = att
		b.count[att.State]++
		r.refillAttempts(att.bucket)
	}
}

func (r *Registration) setAttemptState(att *RegAttempt, state RegAttemptState) {
	att.bucket.count[att.State]--
	att.bucket.count[state]++
	att.State = state
}

func (r *Registration) refillAttempts(b *regBucket) {
	if b.count[Waiting] >= regBucketMaxAttempts {
		return
	}

	// Promote a random replacement.
	for _, att := range b.att {
		if att.State == Standby {
			r.setAttemptState(att, Waiting)
			att.NextTime = r.clock.Now()
			heap.Push(&r.heap, att)
			break
		}
	}
}

// NextRequest returns a registration attempt that should be made.
// If there is nothing to do, this method returns nil.
func (r *Registration) NextRequest() *RegAttempt {
	now := r.clock.Now()
	for len(r.heap) > 0 {
		att := r.heap[0]
		switch att.State {
		case Standby:
			panic("standby attempt in Registration.heap")
		case Registered:
			if att.NextTime >= now {
				r.removeAttempt(att)
			}
		case Waiting:
			return att
		}
	}
	return nil
}

// StartRequest should be called when a registration request is sent for the attempt.
func (r *Registration) StartRequest(att *RegAttempt) {
	if att.State != Waiting {
		panic(fmt.Errorf("StartRequest for attempt with state=%v", att.State))
	}
	heap.Remove(&r.heap, att.index)
	att.index = -1
}

// HandleTicketResponse should be called when a node responds to a registration
// request with a ticket and waiting time.
func (r *Registration) HandleTicketResponse(att *RegAttempt, ticket []byte, waitTime time.Duration) {
	att.Ticket = ticket
	att.NextTime = r.clock.Now().Add(waitTime)
	heap.Push(&r.heap, att)
}

// HandleRegistered should be called when a node confirms topic registration.
func (r *Registration) HandleRegistered(att *RegAttempt, totalWaitTime time.Duration, ttl time.Duration) {
	r.setAttemptState(att, Registered)
	att.NextTime = r.clock.Now().Add(ttl)
	heap.Push(&r.heap, att)
	r.refillAttempts(att.bucket)
}

func (r *Registration) HandleErrorResponse(att *RegAttempt) {
	r.removeAttempt(att)
	r.refillAttempts(att.bucket)
}

func (r *Registration) removeAttempt(att *RegAttempt) {
	nid := att.Node.ID()
	if att.bucket.att[nid] != att {
		panic("trying to delete non-existent attempt")
	}
	if att.index >= 0 {
		heap.Remove(&r.heap, att.index)
		att.index = -1
	}
	delete(att.bucket.att, nid)
	att.bucket.count[att.State]--
}

func (r *Registration) bucket(id enode.ID) *regBucket {
	dist := 256 - enode.LogDist(enode.ID(r.topic), id)
	if dist < len(r.buckets) {
		dist = 0
	} else {
		dist -= len(r.buckets)
	}
	return &r.buckets[dist]
}

// regHeap is a priority queue of registration attempts. This should not be accessed
// directly. Use heap.Push and heap.Pop to add and remove items.
type regHeap []*RegAttempt

func (rh regHeap) Len() int {
	return len(rh)
}

func (rh regHeap) Less(i, j int) bool {
	return rh[i].NextTime < rh[j].NextTime
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
