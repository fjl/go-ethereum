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
	client := startLocalhostV5(t, Config{
		Bootnodes: []*enode.Node{bootnode.Self()},
	})

	client.RegisterTopic(topicindex.TopicID{})

	time.Sleep(10 * time.Second)

	reg := bootnode.LocalTopicNodes(topicindex.TopicID{})
	if len(reg) != 1 || reg[0].ID() != client.localNode.ID() {
		t.Fatal("not registered")
	}
}
