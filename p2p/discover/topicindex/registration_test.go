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
	return enode.SignNull(&r, idAtDistance(base, ld))
}

func intIP(i int) net.IP {
	return net.IP{byte(i), 0, 2, byte(i)}
}
