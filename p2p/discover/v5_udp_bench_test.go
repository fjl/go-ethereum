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
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func BenchmarkHandleTopicMsg(b *testing.B) {
	cfg := Config{
		// disable routing table activities
		RefreshInterval: 1000 * time.Hour,
		PingInterval:    1000 * time.Hour,
	}
	test := newUDPV5Test(b, cfg)
	defer test.close()

	test.remoteaddr = &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 30303}

	buckets := make([]uint, 18)
	for i := 0; i < len(buckets); i++ {
		buckets[i] = 256 - uint(i)
	}

	var (
		src      = test.getNode(test.remotekey, test.remoteaddr)
		srcAddr  = test.remoteaddr
		regtopic = &v5wire.Regtopic{
			ReqID:   []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Topic:   topicindex.TopicID{1, 2, 3, 4, 5},
			ENR:     src.Node().Record(),
			Buckets: buckets,
		}
		topicquery = &v5wire.TopicQuery{
			ReqID:   []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Topic:   topicindex.TopicID{1, 2, 3, 4, 5},
			Buckets: buckets,
		}
	)

	refreshTicket := func() {
		wt := 10 * time.Second
		lastUsed := test.udp.clock.Now() - mclock.AbsTime(wt)
		regtopic.Ticket = test.udp.ticketSealer.Pack(&topicindex.Ticket{
			Topic:          regtopic.Topic,
			WaitTimeIssued: wt,
			LastUsed:       lastUsed,
			FirstIssued:    lastUsed,
		})
	}

	b.Run("topicquery/table-empty", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topic, aux := test.udp.createTopicQueryResponse(src.ID(), srcAddr, topicquery)
			if len(topic) != 0 {
				b.Log(test.udp.Self().ID())
				n, _ := enode.New(enode.ValidSchemes, topic[0][0])
				b.Fatal("non-empty topic nodes:", n.ID())
			}
			if len(aux) != 0 {
				b.Fatal("non-empty aux nodes")
			}
		}
	})
	b.Run("regtopic/table-empty", func(b *testing.B) {
		refreshTicket()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			confirmation, _ := test.udp.createRegtopicResponse(src.ID(), srcAddr, regtopic)
			if confirmation == nil {
				b.Fatal("error in handler")
			}
		}
	})

	const numTableNodes = 50
	for i := 0; i < numTableNodes; i++ {
		self := test.udp.Self().ID()
		n := nodeAtDistance(self, 245+rand.Intn(10), net.IP{127, 0, 0, 1})
		n.livenessChecks = 1
		test.table.addSeenNode(n)
	}

	const numTopicNodes = 200
	for i := 0; i < numTopicNodes; i++ {
		self := test.udp.Self().ID()
		n := nodeAtDistance(self, 246+rand.Intn(10), net.IP{127, 0, 0, 1})
		test.udp.topicTable.Add(&n.Node, regtopic.Topic)
	}

	name := fmt.Sprintf("table-%d-topictable-%d", numTableNodes, numTopicNodes)
	b.Run(name+"/topicquery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topic, aux := test.udp.createTopicQueryResponse(src.ID(), srcAddr, topicquery)
			if len(topic) == 0 {
				b.Fatal("no topic nodes returned")
			}
			if len(aux) == 0 {
				b.Fatal("no aux nodes returned")
			}
		}
	})
	b.Run(name+"/regtopic", func(b *testing.B) {
		refreshTicket()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			confirmation, nodes := test.udp.createRegtopicResponse(src.ID(), srcAddr, regtopic)
			if confirmation == nil {
				b.Fatal("error in handler")
			}
			if len(nodes) == 0 {
				b.Fatal("no aux nodes returned")
			}
		}
	})
}
