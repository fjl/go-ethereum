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
	"bytes"
	"fmt"
	mrand "math/rand"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

var (
	topic1 = TopicID{1, 2, 3, 4, 5, 6}
	topic2 = TopicID{8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
)

func TestTopicTableWait(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	n := newNode()
	wt := tab.Register(n, topic1, 0)

	t.Log("initial wait time", wt)

	wt2 := tab.Register(n, topic1, wt)
	if wt2 != 0 {
		t.Fatal("node not registered after waiting")
	}
}

func TestTopicTableRegisterTwice(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	n := newNode()
	tab.Add(n, topic1)

	wt := tab.Register(n, topic1, 0)
	if wt != 0 {
		t.Fatalf("wrong wait time %v for already-registered node", wt)
	}
}

func TestTopicTableRandomNodes(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	const N = 20

	var topic1nodes []enode.ID
	for i := 0; i < N; i++ {
		n := newNode()
		ok := tab.Add(n, topic1)
		if !ok {
			t.Fatalf("can't add node %d", i)
		}
		topic1nodes = append(topic1nodes, n.ID())
	}
	sortIDs(topic1nodes)

	alwaysTrue := func(*enode.Node) bool { return true }

	check := func(t *testing.T, n int, expectedResults int) {
		result := tab.RandomNodes(topic1, n, alwaysTrue)
		ids := uniqueNodeIDs(result)
		sortIDs(ids)

		if len(ids) != len(result) {
			t.Error("results are not unique")
		}
		if len(ids) != expectedResults {
			t.Errorf("wrong number of results: %d, want %d", len(ids), expectedResults)
		}
	}

	t.Run(fmt.Sprint(N), func(t *testing.T) { check(t, N, N) })
	t.Run(fmt.Sprint(N-1), func(t *testing.T) { check(t, N-1, N-1) })
	t.Run(fmt.Sprint(N+1), func(t *testing.T) { check(t, N+1, N) })
}

func testConfig(t *testing.T) Config {
	return Config{
		AdCacheSize: 20,
		Log:         testlog.Logger(t, log.LvlTrace),
	}
}

func newNode() *enode.Node {
	var r enr.Record
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

func randomNodes(n int) []*enode.Node {
	nodes := make([]*enode.Node, n)
	for i := range nodes {
		nodes[i] = newNode()
	}
	return nodes
}

func uniqueNodeIDs(nodes []*enode.Node) []enode.ID {
	byID := make(map[enode.ID]struct{}, len(nodes))
	for _, n := range nodes {
		byID[n.ID()] = struct{}{}
	}
	ids := make([]enode.ID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	return ids
}

func sortIDs(ids []enode.ID) {
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})
}
