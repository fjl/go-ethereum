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
