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
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func TestTopicReg(t *testing.T) {
	bootnode := startLocalhostV5(t, Config{})
	client := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})

	client.RegisterTopic(topicindex.TopicID{})

	time.Sleep(10 * time.Second)

	reg := bootnode.LocalTopicNodes(topicindex.TopicID{})
	if len(reg) != 1 || reg[0].ID() != client.localNode.ID() {
		t.Fatal("not registered")
	}
}

// This is an end-to-end test of topic search.
func TestTopicSearch(t *testing.T) {
	topic := topicindex.TopicID{1, 1, 1, 1}

	// Create network of four nodes.
	bootnode := startLocalhostV5(t, Config{})
	node1 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})
	node2 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})
	node3 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})

	// Add registrations of two other nodes in the topic table of node1.
	node1.topicTable.Add(bootnode.Self(), topic)
	node1.topicTable.Add(node3.Self(), topic)

	// Attempt to discover the registrations from yet another node.
	it := node2.TopicSearch(topic)
	defer it.Close()
	nodes := enode.ReadNodes(it, 2)
	t.Log("found nodes:", nodes)
}
