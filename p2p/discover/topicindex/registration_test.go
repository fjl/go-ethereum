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
	"net"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// This test checks basic assignment of nodes into registration buckets.
func TestRegistrationBuckets(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	var (
		far256  = nodesAtDistance(enode.ID(topic1), 256, 3)
		far255  = nodesAtDistance(enode.ID(topic1), 255, 3)
		close5  = nodesAtDistance(enode.ID(topic1), 5, 1)
		close20 = nodesAtDistance(enode.ID(topic1), 20, 1)
	)
	r.AddNodes(nil, far256)
	r.AddNodes(nil, far255)
	r.AddNodes(nil, close5)
	r.AddNodes(nil, close20)

	last := len(r.buckets) - 1
	if !rbContainsAll(r.buckets[0], far256) {
		t.Fatalf("far256 nodes missing in bucket[%d]", 0)
	}
	if !rbContainsAll(r.buckets[1], far255) {
		t.Fatalf("far255 nodes missing in bucket[%d]", 1)
	}
	if !rbContainsAll(r.buckets[last], close5) {
		t.Fatalf("close5 nodes missing in bucket[%d]", last)
	}
	if !rbContainsAll(r.buckets[last], close20) {
		t.Fatalf("close20 nodes missing in bucket[%d]", last-20)
	}
}

func rbContainsAll(b regBucket, nodes []*enode.Node) bool {
	for _, n := range nodes {
		if _, ok := b.att[n.ID()]; !ok {
			return false
		}
	}
	return true
}

// This checks that the one-per-bucket rule is applied in AddNodes.
func TestRegistrationOnePerBucketCheck(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	src := nodeAtDistance(enode.ID(topic1), 255, intIP(1))

	// Attempt to insert multiple nodes in the same bucket.
	// Only one of them should actually be added.
	nodes := nodesAtDistance(enode.ID(topic1), 200, 10)
	r.AddNodes(src, nodes)
	if r.NodeCount() > 1 {
		t.Fatal("too many nodes added")
	}
}

// This checks that the per-bucket IP limit is applied in AddNodes.
func TestRegistrationIPCheck(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	src1 := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	src2 := nodeAtDistance(enode.ID(topic1), 255, intIP(1))

	// Attempt to insert nodes with same IP in one bucket.
	// Only one of them should actually be added.
	// This needs to be done across multiple AddNodes calls to avoid
	// the one-per-bucket check.

	node1 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	node2 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 2})
	r.AddNodes(src1, []*enode.Node{node1})
	r.AddNodes(src2, []*enode.Node{node2})

	if r.NodeCount() > 1 {
		t.Fatal("too many nodes added")
	}
}

// This test checks that registration attempts are created for found nodes.
func TestRegistrationRequests(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	if req := r.Update(); req != nil {
		t.Fatal("request spawned on fresh Registration")
	}

	// Deliver some nodes.
	target := enode.ID(r.Topic())
	for i := 50; i < 256; i++ {
		nodes := nodesAtDistance(target, i, 5)
		r.AddNodes(nil, nodes)
	}

	req := r.Update()
	if req == nil {
		t.Fatal("no request scheduled")
	}
	if r.Update() != req {
		t.Fatal("top request changed")
	}

	r.StartRequest(req)
	if r.Update() == req {
		t.Fatal("top request not removed")
	}
	r.HandleRegistered(req, cfg.AdLifetime)
}

// This test checks that registration attempts expire after the lifetime
// of the ad runs out.
func TestRegistrationExpiry(t *testing.T) {
	simclock := new(mclock.Simulated)

	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdLifetime = 20
	r := NewRegistration(topic1, cfg)

	// Deliver some nodes.
	node := nodesAtDistance(enode.ID(r.Topic()), 30, 1)
	r.AddNodes(nil, node)

	// A registration attempt should be created.
	att := r.Update()
	if att == nil {
		t.Fatal("no request scheduled")
	}
	if att.State != Waiting {
		t.Fatal("attempt should be in state", Waiting, "but has state", att.State)
	}

	// Mark registration to successful.
	r.StartRequest(att)
	r.HandleRegistered(att, cfg.AdLifetime)

	// NextUpdateTime should now return the expiry time of the ad.
	now := simclock.Now()
	if next := r.NextUpdateTime(); next != now.Add(cfg.AdLifetime) {
		t.Fatal("wrong next update time:", next)
	}

	// The attempt should be removed when the ad expires.
	simclock.Run(cfg.AdLifetime)
	if a := r.Update(); a != nil {
		t.Log(spew.Sdump(a))
		t.Fatal("Update returned an attempt, but nothing to do.")
	}
	if r.heap.Len() > 0 {
		t.Fatal("attempt not removed")
	}

	// Re-add the node.
	simclock.Run(1 * time.Second)
	r.AddNodes(nil, node)

	// It should get scheduled for registration again.
	att = r.Update()
	if att == nil {
		t.Fatal("no request scheduled")
	}
	if att.State != Waiting {
		t.Fatal("attempt should be in state", Waiting, "but has state", att.State)
	}
}

// nodesAtDistance creates n nodes for which enode.LogDist(base, node.ID()) == ld.
func nodesAtDistance(base enode.ID, ld int, n int) []*enode.Node {
	results := make([]*enode.Node, n)
	for i := range results {
		results[i] = nodeAtDistance(base, ld, intIP(i))
	}
	return results
}

// nodeAtDistance creates a node for which enode.LogDist(base, n.id) == ld.
func nodeAtDistance(base enode.ID, ld int, ip net.IP) *enode.Node {
	var r enr.Record
	r.Set(enr.IP(ip))
	return enode.SignNull(&r, enode.RandomID(base, ld))
}

func intIP(i int) net.IP {
	return net.IP{byte(i), 0, 2, byte(i)}
}
