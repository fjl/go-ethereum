// Copyright 2015 The go-ethereum Authors
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

package p2p

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// This test checks that dynamic dials are launched from discovery results.
func TestDialStateDynDial(t *testing.T) {
	t.Parallel()

	config := dialerConfig{maxActiveDials: 3, maxDynPeers: 4}
	runDialTest(t, config, []dialTestRound{
		// 9 nodes are discovered in the first round.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "")}},
			},
			discovered: []*enode.Node{
				newNode(uintID(0), "127.0.0.1"), // not dialed because already connected as static peer
				newNode(uintID(2), "127.0.0.1"), // not dialed because already connected as dynamic peer
				newNode(uintID(3), "127.0.0.1"),
				newNode(uintID(4), "127.0.0.1"),
				newNode(uintID(5), "127.0.0.1"),
				newNode(uintID(6), "127.0.0.1"), // not tried because max dyn dials is 3
				newNode(uintID(7), "127.0.0.1"), // ...
				newNode(uintID(8), "127.0.0.1"), // ...
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(3), "127.0.0.1"),
				newNode(uintID(4), "127.0.0.1"),
				newNode(uintID(5), "127.0.0.1"),
			},
		},

		// Dials from previous round complete and new ones are launched up to the limit.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "")}},
			},
			completed: map[enode.ID]error{
				uintID(3): nil,
				uintID(4): nil,
				uintID(5): nil,
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(6), "127.0.0.1"),
				newNode(uintID(7), "127.0.0.1"),
				newNode(uintID(8), "127.0.0.1"),
			},
		},

		// More dials complete, filling the last remaining peer slot.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(6), "")}},
			},
			completed: map[enode.ID]error{
				uintID(6): nil,
				uintID(7): nil,
				uintID(8): nil,
			},
			discovered: []*enode.Node{
				newNode(uintID(9), "127.0.0.1"),
			},
		},

		// No new dial tasks are launched in the this round because
		// maxDynDials has been reached.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(6), "")}},
			},
		},

		// In this round, the peer with id 2 drops off. The query
		// results from last discovery lookup are reused.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(6), "")}},
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(9), ""),
			},
		},

		// This round just completes the dial.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(6), "")}},
			},
			completed: map[enode.ID]error{
				uintID(9): nil,
			},
		},
	})
}

// This test checks that candidates that do not match the netrestrict list are not dialed.
func TestDialStateNetRestrict(t *testing.T) {
	t.Parallel()

	nodes := []*enode.Node{
		newNode(uintID(1), "127.0.0.1:30303"),
		newNode(uintID(2), "127.0.0.2:30303"),
		newNode(uintID(3), "127.0.0.3:30303"),
		newNode(uintID(4), "127.0.0.4:30303"),
		newNode(uintID(5), "127.0.2.5:30303"),
		newNode(uintID(6), "127.0.2.6:30303"),
		newNode(uintID(7), "127.0.2.7:30303"),
		newNode(uintID(8), "127.0.2.8:30303"),
	}
	config := dialerConfig{
		netRestrict:    new(netutil.Netlist),
		maxActiveDials: 10,
		maxDynPeers:    10,
	}
	config.netRestrict.Add("127.0.2.0/24")
	runDialTest(t, config, []dialTestRound{
		{
			discovered:   nodes,
			wantNewDials: nodes[4:8],
		},
		{
			completed: map[enode.ID]error{
				nodes[4].ID(): nil,
				nodes[5].ID(): nil,
				nodes[6].ID(): nil,
				nodes[7].ID(): nil,
			},
		},
	})
}

// This test checks that static dials are launched.
func TestDialStateStaticDial(t *testing.T) {
	t.Parallel()

	config := dialerConfig{
		maxActiveDials: 2,
		maxDynPeers:    3,
	}
	runDialTest(t, config, []dialTestRound{
		// Static dials are launched for the nodes that
		// aren't yet connected.
		{
			peers: []*Peer{
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
			update: func(d *dialer2) {
				d.addStatic(newNode(uintID(1), "127.0.0.1:30303"))
				d.addStatic(newNode(uintID(2), "127.0.0.2:30303"))
				d.addStatic(newNode(uintID(3), "127.0.0.3:30303"))
				d.addStatic(newNode(uintID(4), "127.0.0.4:30303"))
				d.addStatic(newNode(uintID(5), "127.0.0.5:30303"))
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(3), "127.0.0.3:30303"),
				newNode(uintID(4), "127.0.0.4:30303"),
			},
		},
		// Dial to 3 completes, filling the last peer slot. No new tasks.
		{
			peers: []*Peer{
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), "127.0.0.1:30303")}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "127.0.0.2:30303")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), "127.0.0.3:30303")}},
			},
			completed: map[enode.ID]error{
				uintID(3): nil,
				uintID(4): nil,
			},
		},
		// Peer 1 drops. New dials are launched to fill the slot.
		{
			peers: []*Peer{
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), "127.0.0.2:30303")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), "127.0.0.3:30303")}},
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(1), "127.0.0.1:30303"),
				newNode(uintID(5), "127.0.0.5:30303"),
			},
		},
	})
}

// This test checks that past dials are not retried for some time.
func TestDialStateHistory(t *testing.T) {
	t.Parallel()

	config := dialerConfig{
		maxActiveDials: 3,
		maxDynPeers:    3,
	}
	runDialTest(t, config, []dialTestRound{
		// Static dials are launched for the nodes that aren't yet connected.
		{
			update: func(d *dialer2) {
				d.addStatic(newNode(uintID(1), "127.0.0.1"))
				d.addStatic(newNode(uintID(2), "127.0.0.2"))
				d.addStatic(newNode(uintID(3), "127.0.0.3"))
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(1), "127.0.0.1"),
				newNode(uintID(2), "127.0.0.2"),
				newNode(uintID(3), "127.0.0.3"),
			},
		},
		// No new tasks are launched in this round because all static
		// nodes are either connected or still being dialed.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
			completed: map[enode.ID]error{
				uintID(1): nil,
				uintID(2): nil,
				uintID(3): errors.New("oops"),
			},
		},
		// Nothing happens in this round because we're waiting for
		// node 3's history entry to expire.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
		},
		// Still waiting...
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
		},
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
		},
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
		},
		// The cache entry for node 3 has expired and is retried.
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), "127.0.0.1")}},
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), "127.0.0.2")}},
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(3), "127.0.0.3"),
			},
		},
	})
}

func TestDialStateResolve(t *testing.T) {
	t.Parallel()

	config := dialerConfig{
		maxActiveDials: 1,
		maxDynPeers:    1,
	}
	node := newNode(uintID(1), "")
	resolved := newNode(uintID(1), "127.0.55.234:30303")
	runDialTest(t, config, []dialTestRound{
		{
			update: func(d *dialer2) {
				d.addStatic(node)
			},
			wantResolves: map[enode.ID]*enode.Node{
				uintID(1): resolved,
			},
			wantNewDials: []*enode.Node{
				node,
			},
		},
		{
			completed: map[enode.ID]error{
				uintID(1): errors.New("oops"),
			},
			wantResolves: map[enode.ID]*enode.Node{
				uintID(1): resolved,
			},
			wantNewDials: []*enode.Node{
				resolved,
			},
		},
	})
}

// -------
// Code below here is the framework for the tests above.

type dialTestRound struct {
	peers        []*Peer            // current peer set
	update       func(*dialer2)     // called at beginning of round
	discovered   []*enode.Node      // newly discovered nodes
	completed    map[enode.ID]error // dials that complete in this round
	wantResolves map[enode.ID]*enode.Node
	wantNewDials []*enode.Node // dials that should be launched in this round
}

func runDialTest(t *testing.T, config dialerConfig, rounds []dialTestRound) {
	var (
		clock    = new(mclock.Simulated)
		iterator = newDialTestIterator()
		dialer   = newDialTestDialer()
		resolver = new(dialTestResolver)
	)

	config.clock = clock
	config.dialer = dialer
	config.resolver = resolver
	config.log = testlog.Logger(t, log.LvlTrace)
	setup := func(net.Conn, connFlag, *enode.Node) error {
		return nil
	}
	dialstate := newDialer2(config, iterator, setup)
	defer dialstate.stop()

	pm := func(ps []*Peer) map[enode.ID]*Peer {
		m := make(map[enode.ID]*Peer)
		for _, p := range ps {
			m[p.ID()] = p
		}
		return m
	}

	for i, round := range rounds {
		t.Logf("round %d", i)
		resolver.setAnswers(round.wantResolves)
		dialstate.setPeers(pm(round.peers))
		if round.update != nil {
			round.update(dialstate)
		}
		iterator.addNodes(round.discovered)

		if err := dialer.completeDials(round.completed); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if err := dialer.waitForDials(round.wantNewDials); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if !resolver.checkCalls() {
			t.Fatalf("unexpected calls to Resolve: %v", resolver.calls)
		}

		clock.Run(16 * time.Second)
	}
}

// dialTestIterator is the input iterator for dialer tests. This works a bit like a channel
// with infinite buffer: nodes are added to the buffer with addNodes, which unblocks Next
// and returns them from the iterator.
type dialTestIterator struct {
	cur *enode.Node

	mu     sync.Mutex
	buf    []*enode.Node
	cond   *sync.Cond
	closed bool
}

func newDialTestIterator() *dialTestIterator {
	it := &dialTestIterator{}
	it.cond = sync.NewCond(&it.mu)
	return it
}

// addNodes adds nodes to the iterator buffer and unblocks Next.
func (it *dialTestIterator) addNodes(nodes []*enode.Node) {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.buf = append(it.buf, nodes...)
	it.cond.Signal()
}

// Node returns the current node.
func (it *dialTestIterator) Node() *enode.Node {
	return it.cur
}

// Next moves to the next node.
func (it *dialTestIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.cur = nil
	for len(it.buf) == 0 && !it.closed {
		it.cond.Wait()
	}
	if it.closed {
		return false
	}
	it.cur = it.buf[0]
	copy(it.buf[:], it.buf[1:])
	it.buf = it.buf[:len(it.buf)-1]
	return true
}

// Close ends the iterator, unblocking Next.
func (it *dialTestIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.buf = nil
	it.cond.Signal()
}

// dialTestDialer is the NodeDialer used by runDialTest.
type dialTestDialer struct {
	init    chan *dialTestReq
	blocked map[enode.ID]*dialTestReq
}

type dialTestReq struct {
	n       *enode.Node
	unblock chan error
}

func newDialTestDialer() *dialTestDialer {
	return &dialTestDialer{
		init:    make(chan *dialTestReq),
		blocked: make(map[enode.ID]*dialTestReq),
	}
}

// Dial implements NodeDialer.
func (d *dialTestDialer) Dial(ctx context.Context, n *enode.Node) (net.Conn, error) {
	req := &dialTestReq{n: n, unblock: make(chan error, 1)}
	select {
	case d.init <- req:
		select {
		case err := <-req.unblock:
			pipe, _ := net.Pipe()
			return pipe, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// waitForDials waits for calls to Dial with the given nodes as argument.
// Those calls will be held blocking until completeDials is called with the same nodes.
func (d *dialTestDialer) waitForDials(nodes []*enode.Node) error {
	waitset := make(map[enode.ID]struct{})
	for _, n := range nodes {
		waitset[n.ID()] = struct{}{}
	}
	timeout := time.NewTimer(1 * time.Second)
	defer timeout.Stop()

	for len(waitset) > 0 {
		select {
		case req := <-d.init:
			if _, ok := waitset[req.n.ID()]; !ok {
				return fmt.Errorf("attempt to dial unexpected node %v", req.n.ID())
			}
			delete(waitset, req.n.ID())
			d.blocked[req.n.ID()] = req
		case <-timeout.C:
			var waitlist []enode.ID
			for id := range waitset {
				waitlist = append(waitlist, id)
			}
			return fmt.Errorf("timed out waiting for dials to %v", waitlist)
		}
	}

	return d.checkUnexpectedDial()
}

func (d *dialTestDialer) checkUnexpectedDial() error {
	select {
	case req := <-d.init:
		return fmt.Errorf("attempt to dial unexpected node %v", req.n.ID())
	case <-time.After(150 * time.Millisecond):
		return nil
	}
}

// completeDials unblocks calls to Dial for the given nodes.
func (d *dialTestDialer) completeDials(nodes map[enode.ID]error) error {
	for id, err := range nodes {
		req := d.blocked[id]
		if req == nil {
			return fmt.Errorf("can't complete dial to %v", id)
		}
		req.unblock <- err
	}
	return nil
}

// dialTestResolver tracks calls to resolve.
type dialTestResolver struct {
	mu      sync.Mutex
	calls   []enode.ID
	answers map[enode.ID]*enode.Node
}

func (t *dialTestResolver) setAnswers(m map[enode.ID]*enode.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.answers = m
	t.calls = nil
}

func (t *dialTestResolver) checkCalls() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range t.calls {
		if _, ok := t.answers[id]; !ok {
			return false
		}
	}
	return true
}

func (t *dialTestResolver) Resolve(n *enode.Node) *enode.Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.calls = append(t.calls, n.ID())
	return t.answers[n.ID()]
}
