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

package discover

import (
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

var (
	testTopic1 = topicindex.TopicID{1, 1, 1, 1}
)

func TestTopicReg(t *testing.T) {
	bootnode := startLocalhostV5(t, Config{})
	client := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})

	client.RegisterTopic(topicindex.TopicID{}, 0)

	time.Sleep(10 * time.Second)

	reg := bootnode.LocalTopicNodes(topicindex.TopicID{})
	if len(reg) != 1 || reg[0].ID() != client.localNode.ID() {
		t.Fatal("not registered")
	}
}

// This test checks that topic registration will pick up new nodes
// when they are added to the main node table.
func TestTopicRegNodeTableUpdates(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	var (
		key1, ln1 = test.createNode(1)
		key2, ln2 = test.createNode(2)
	)

	// node1 is in the local table before registration begins.
	test.table.addVerifiedNode(wrapNode(ln1.Node()))

	// Catch registration attempt with node1.
	test.udp.RegisterTopic(testTopic1, 1)
	test.waitPacketOut(func(p *v5wire.Regtopic, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetInFrom(key1, addr, &v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   nil, // successfully registered
			WaitTime: 900000,
		})
	})

	// Now add node2 and wait for the table to verify its liveness.
	test.table.addSeenNode(wrapNode(ln2.Node()))
	test.waitPacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		if !addr.IP.Equal(ln2.Node().IP()) {
			t.Fatal("PING to wrong node", addr)
		}
		test.packetInFrom(key2, addr, &v5wire.Pong{
			ReqID: p.ReqID,
		})
	})

	// A registration attempt should be made with node2.
	test.waitPacketOut(func(p *v5wire.Regtopic, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   nil, // successfully registered
			WaitTime: 900000,
		})
	})

	count := test.udp.topicSys.reg[testTopic1].state.NodeCount()
	if count != 2 {
		t.Fatal("wrong node count in reg table:", count)
	}
}

// This is an end-to-end test of topic search.
func TestTopicSearch(t *testing.T) {
	topic := topicindex.TopicID{1, 1, 1, 1}

	// Create network of four nodes.
	node0 := startLocalhostV5(t, Config{})
	node1 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node2 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node3 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	defer func() {
		for _, node := range []*UDPv5{node0, node1, node2, node3} {
			node.Close()
		}
	}()

	// Add registrations of two other nodes in the topic table of node1.
	node1.topicTable.Add(node0.Self(), topic)
	node1.topicTable.Add(node3.Self(), topic)

	// Attempt to discover the registrations from yet another node.
	it := node2.TopicSearch(topic, 0)
	defer it.Close()
	nodes := enode.ReadNodes(it, 2)
	t.Log("found nodes:", nodes)
}
