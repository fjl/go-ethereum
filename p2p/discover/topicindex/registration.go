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
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
	// IP subnet limit.
	regBucketSubnet, regBucketIPLimit = 24, 1

	// regTableDepth is the number of buckets kept in the registration table.
	//
	// The table only keeps nodes at logdist(topic, n) > (256 - regTableDepth).
	// Should there be any nodes which are closer than this, they just go into the last
	// (closest) bucket.
	regTableDepth = 40
)

// Registration is the state associated with registering in a single topic.
type Registration struct {
	topic TopicID
	cfg   Config
	log   log.Logger

	// Note: registration buckets are ordered close -> far.
	buckets [regTableDepth]regBucket
	heap    regHeap

	bucketCheck map[int]struct{}
}

//go:generate go run golang.org/x/tools/cmd/stringer@latest -type RegAttemptState

// RegAttemptState is the state of a registration attempt on a node.
type RegAttemptState int

type regBucket struct {
	dist  int
	att   map[enode.ID]*RegAttempt
	count [nRegStates]int

	ips netutil.DistinctNetSet
}

const (
	Standby RegAttemptState = iota
	Waiting
	Registered
	nRegStates int = iota
)

// RegAttempt contains the state of the registration process against
// a single registrar node.
type RegAttempt struct {
	State RegAttemptState

	// NextTime is when the next action related to this attempt must occur.
	//
	// In state 'Waiting'
	//     it is the time of the next registration attempt.
	// In state 'Registered'
	//     it is the time when the ad expires.
	NextTime mclock.AbsTime

	// Node is the registrar node.
	Node *enode.Node

	// Ticket contains the ticket data returned by the last registration call.
	Ticket []byte

	// totalWaitTime is the time spent waiting so far.
	totalWaitTime time.Duration

	// reqCount tracks the number of registration requests sent.
	reqCount int

	index  int // index in regHeap
	bucket *regBucket
}

func NewRegistration(topic TopicID, cfg Config) *Registration {
	cfg = cfg.withDefaults()
	r := &Registration{
		topic:       topic,
		cfg:         cfg,
		log:         cfg.Log.New("topic", topic),
		bucketCheck: make(map[int]struct{}, regTableDepth),
	}
	dist := 256
	for i := range r.buckets {
		r.buckets[i] = regBucket{
			att:  make(map[enode.ID]*RegAttempt),
			dist: dist - (len(r.buckets) - 1) + i,
			ips:  netutil.DistinctNetSet{Subnet: regBucketSubnet, Limit: regBucketIPLimit},
		}
	}
	return r
}

// Topic returns the topic being registered for.
func (r *Registration) Topic() TopicID {
	return r.topic
}

// NodeCount returns the number of unique nodes across all buckets.
func (r *Registration) NodeCount() int {
	sum := 0
	for _, b := range r.buckets {
		for _, c := range b.count {
			sum += c
		}
	}
	return sum
}

// AddNodes notifies the registration process about found nodes.
//
// 'src' is the source of the nodes.
func (r *Registration) AddNodes(src *enode.Node, nodes []*enode.Node) {
	// Clear the one-per-bucket checker.
	for i := range r.bucketCheck {
		delete(r.bucketCheck, i)
	}

	// Add the nodes.
	for _, n := range nodes {
		id := n.ID()
		if id == r.cfg.Self {
			continue
		}

		bi := r.bucketIndex(id)
		b := &r.buckets[bi]
		attempt, ok := b.att[id]
		if ok {
			// There is already an attempt scheduled with this node.
			// Update the record if newer.
			if attempt.Node.Seq() < n.Seq() {
				attempt.Node = n
			}
			continue
		}

		if src != nil {
			// The node is supplied by a remote registrar. Enforce 'one-per-bucket' rule:
			// in the list given by the registrar, there should be at most one node for
			// every registration bucket. This avoids attacks where a single registrar can
			// dominate a bucket.
			if _, ok := r.bucketCheck[bi]; ok {
				r.log.Debug("Ignoring registration node", "id", n.ID(), "reason", "one-per-bucket-rule")
				continue
			}
			r.bucketCheck[bi] = struct{}{}
		}

		if b.count[Standby] >= r.cfg.RegBucketStandbyLimit {
			// There are enough replacements already.
			continue
		}

		ip := n.IP()
		if ip != nil && !netutil.IsLAN(ip) && !b.ips.Add(n.IP()) {
			// IP doesn't fit in bucket limit.
			r.log.Debug("Ignoring registration node", "id", n.ID(), "reason", "iplimit")
			continue
		}

		// Create a new attempt.
		att := &RegAttempt{Node: n, bucket: b, index: -1}
		b.att[id] = att
		b.count[att.State]++
		r.refillAttempts(att.bucket)
	}
}

func (r *Registration) setAttemptState(att *RegAttempt, state RegAttemptState) {
	att.bucket.count[att.State]--
	att.bucket.count[state]++
	r.log.Trace("Registration attempt state changed", "id", att.Node.ID(), "state", state, "prev", att.State)
	att.State = state
}

// refillAttempts promotes a registrar node from Standby to Waiting.
// This must be called after every potential attempt state change in the bucket.
func (r *Registration) refillAttempts(b *regBucket) {
	if b.count[Waiting] >= r.cfg.RegBucketSize {
		// Enough attempts in state 'Waiting'.
		return
	}

	for _, att := range b.att {
		if att.State == Standby {
			r.setAttemptState(att, Waiting)
			att.NextTime = r.cfg.Clock.Now()
			heap.Push(&r.heap, att)
			break
		}
	}
}

// NextUpdateTime returns the next time Update should be called.
func (r *Registration) NextUpdateTime() mclock.AbsTime {
	if len(r.heap) > 0 {
		att := r.heap[0]
		switch att.State {
		case Standby:
			panic("standby attempt in Registration.heap")
		case Registered, Waiting:
			return att.NextTime
		}
	}
	return Never
}

// Update processes the attempt queue and returns the next attempt in state 'Waiting'.
func (r *Registration) Update() *RegAttempt {
	now := r.cfg.Clock.Now()
	for len(r.heap) > 0 {
		att := r.heap[0]
		switch att.State {
		case Standby:
			panic("standby attempt in Registration.heap")
		case Registered:
			if now >= att.NextTime {
				r.removeAttempt(att, "expired")
				r.refillAttempts(att.bucket)
			}
		case Waiting:
			if now >= att.NextTime {
				return att
			}
			return nil
		}
	}
	return nil
}

// StartRequest should be called when a registration request is sent for the attempt.
func (r *Registration) StartRequest(att *RegAttempt) {
	if att.index < 0 {
		panic(fmt.Errorf("bad attempt index %d in StartRequest", att.index))
	}
	if att.State != Waiting {
		panic(fmt.Errorf("StartRequest for attempt with state=%v", att.State))
	}
	heap.Remove(&r.heap, att.index)
	att.index = -2
	att.reqCount++
}

func (r *Registration) validate(att *RegAttempt) {
	if att.index != -2 {
		id := att.Node.ID().Bytes()
		panic(fmt.Errorf("attempt (node %x state %s) has bad index %d", id[:8], att.State, att.index))
	}
}

// HandleTicketResponse should be called when a node responds to a registration
// request with a ticket and waiting time.
func (r *Registration) HandleTicketResponse(att *RegAttempt, ticket []byte, waitTime time.Duration) {
	r.validate(att)
	att.totalWaitTime += waitTime

	// Drop the attempt when the registrar makes us wait for longer than AdLifetime. This
	// works because the entire ad cache will have been rotated after one lifetime, and so
	// the registrar must be misbehaving if they didn't accept
	if att.reqCount > 1 && att.totalWaitTime > r.cfg.RegAttemptTimeout {
		r.removeAttempt(att, "wtime-too-high")
		return
	}

	// TODO: should a maximum number of retries be enforced here?

	att.Ticket = ticket
	att.NextTime = r.cfg.Clock.Now().Add(waitTime)
	heap.Push(&r.heap, att)
}

// HandleRegistered should be called when a node confirms topic registration.
func (r *Registration) HandleRegistered(att *RegAttempt, ttl time.Duration) {
	r.validate(att)

	// Prevent registrar from announcing an out-of-bounds ad lifetime.
	if ttl > r.cfg.AdLifetime {
		ttl = r.cfg.AdLifetime
	}

	r.log.Trace("Topic registration successful", "id", att.Node.ID(), "adlifetime", ttl)
	r.setAttemptState(att, Registered)
	att.NextTime = r.cfg.Clock.Now().Add(ttl)
	heap.Push(&r.heap, att)

	r.refillAttempts(att.bucket)
}

// HandleErrorResponse should be called when a registration attempt fails.
func (r *Registration) HandleErrorResponse(att *RegAttempt, err error) {
	r.validate(att)

	r.log.Debug("Topic registration failed", "id", att.Node.ID(), "err", err)
	r.removeAttempt(att, "error")
	r.refillAttempts(att.bucket)
}

func (r *Registration) removeAttempt(att *RegAttempt, reason string) {
	nid := att.Node.ID()
	if att.bucket.att[nid] != att {
		panic("trying to delete non-existent attempt")
	}
	r.log.Trace("Removing registration attempt", "id", att.Node.ID(), "state", att.State, "reason", reason)
	if att.index >= 0 {
		heap.Remove(&r.heap, att.index)
	}
	delete(att.bucket.att, nid)
	att.bucket.count[att.State]--
}

func (r *Registration) bucket(id enode.ID) *regBucket {
	return &r.buckets[r.bucketIndex(id)]
}

func (r *Registration) bucketIndex(id enode.ID) int {
	dist := enode.LogDist(enode.ID(r.topic), id)
	index := dist - 256 + (len(r.buckets) - 1)
	if index < 0 {
		index = 0
	}
	return index
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
