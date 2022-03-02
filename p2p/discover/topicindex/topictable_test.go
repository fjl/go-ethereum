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
	"fmt"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

var (
	topic1 = TopicID{1, 2, 3, 4, 5, 6}
	topic2 = TopicID{8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
)

func TestTopicTableRegister1(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(enode.ID{}, cfg)

	n := newNode()
	ticket := Ticket{Topic: topic1, TotalWaitTime: 1 * time.Second}
	wt := tab.Register(n, ticket)
	fmt.Println(wt)

	spew.Dump(tab)
}

func testConfig(t *testing.T) Config {
	return Config{
		TableLimit: 20,
		Log:        testlog.Logger(t, log.LvlTrace),
	}
}

func newNode() *enode.Node {
	var r enr.Record
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

func generateNodes(n int) []*enode.Node {
	nodes := make([]*enode.Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = newNode()
	}
	return nodes
}
