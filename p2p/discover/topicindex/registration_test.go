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

func TestRegistrationBuckets(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	var (
		far256  = nodesAtDistance(enode.ID(topic1), 256, 3)
		far255  = nodesAtDistance(enode.ID(topic1), 255, 3)
		close5  = nodesAtDistance(enode.ID(topic1), 5, 1)
		close20 = nodesAtDistance(enode.ID(topic1), 20, 1)
	)
	r.AddNodes(far256)
	r.AddNodes(far255)
	r.AddNodes(close5)
	r.AddNodes(close20)

	last := len(r.buckets) - 1
	if !rbContainsAll(r.buckets[last], far256) {
		t.Fatalf("far256 nodes missing in bucket[%d]", last)
	}
	if !rbContainsAll(r.buckets[last-1], far255) {
		t.Fatalf("far255 nodes missing in bucket[%d]", last-1)
	}
	if !rbContainsAll(r.buckets[0], close5) {
		t.Fatalf("close5 nodes missing in bucket[%d]", 0)
	}
	if !rbContainsAll(r.buckets[0], close20) {
		t.Fatalf("close20 nodes missing in bucket[%d]", 0)
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

// This test checks that registration attempts are created for found nodes.
func TestRegistrationRequests(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	if req := r.Update(); req != nil {
		t.Fatal("request spawned on fresh Registration")
	}

	target := r.LookupTarget()
	t.Logf("lookup target: %x", target[:])

	// Deliver some nodes.
	for i := 50; i < 256; i++ {
		nodes := nodesAtDistance(target, i, 5)
		r.AddNodes(nodes)
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
	r.HandleRegistered(req, 1*time.Second, 10*time.Minute)
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
	r.AddNodes(node)

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
	r.HandleRegistered(att, 1*time.Second, cfg.AdLifetime)

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
	r.AddNodes(node)

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
