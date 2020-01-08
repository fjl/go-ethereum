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
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// This test checks that dynamic dials are launched from discovery results.
func TestDialStateDynDial(t *testing.T) {
	runDialTest(t, []dialTestRound{
		{
			peers: []*Peer{
				{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
				{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
			},
			discovered: []*enode.Node{
				newNode(uintID(0), net.IP{127, 0, 0, 1}), // not dialed because it's already connected as static peer
				newNode(uintID(2), net.IP{127, 0, 0, 1}), // not dialed because it's already connected as dynamic peer
				newNode(uintID(3), net.IP{127, 0, 0, 1}),
				newNode(uintID(4), net.IP{127, 0, 0, 1}),
				newNode(uintID(5), net.IP{127, 0, 0, 1}),
				newNode(uintID(6), net.IP{127, 0, 0, 1}), // these are not tried because max dyn dials is 5
				newNode(uintID(7), net.IP{127, 0, 0, 1}), // ...
			},
			wantNewDials: []*enode.Node{
				newNode(uintID(3), net.IP{127, 0, 0, 1}),
				newNode(uintID(4), net.IP{127, 0, 0, 1}),
				newNode(uintID(5), net.IP{127, 0, 0, 1}),
			},
		},

		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
		// 	},
		// 	discovered: []*enode.Node{},
		// },

		// // some of the dials complete but no new ones are launched yet because
		// // the sum of active dial count and dynamic peer count is == maxDynDials.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(3), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(4), nil)}},
		// 	},
		// 	done: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(3), nil)},
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(4), nil)},
		// 	},
		// },
		// // No new dial tasks are launched in the this round because
		// // maxDynDials has been reached.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(3), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(4), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(5), nil)}},
		// 	},
		// 	done: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(5), nil)},
		// 	},
		// 	new: []task{
		// 		&waitExpireTask{Duration: 19 * time.Second},
		// 	},
		// },
		// // In this round, the peer with id 2 drops off. The query
		// // results from last discovery lookup are reused.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(3), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(4), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(5), nil)}},
		// 	},
		// 	new: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(6), nil)},
		// 	},
		// },
		// // More peers (3,4) drop off and dial for ID 6 completes.
		// // The last query result from the discovery lookup is reused
		// // and a new one is spawned because more candidates are needed.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(5), nil)}},
		// 	},
		// 	done: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(6), nil)},
		// 	},
		// 	new: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(7), nil)},
		// 		&discoverTask{want: 2},
		// 	},
		// },
		// // Peer 7 is connected, but there still aren't enough dynamic peers
		// // (4 out of 5). However, a discovery is already running, so ensure
		// // no new is started.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(5), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(7), nil)}},
		// 	},
		// 	done: []task{
		// 		&dialTask{flags: dynDialedConn, dest: newNode(uintID(7), nil)},
		// 	},
		// },
		// // Finish the running node discovery with an empty set. A new lookup
		// // should be immediately requested.
		// {
		// 	peers: []*Peer{
		// 		{rw: &conn{flags: staticDialedConn, node: newNode(uintID(0), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(5), nil)}},
		// 		{rw: &conn{flags: dynDialedConn, node: newNode(uintID(7), nil)}},
		// 	},
		// 	done: []task{
		// 		&discoverTask{},
		// 	},
		// 	new: []task{
		// 		&discoverTask{want: 2},
		// 	},
		// },
	})
}

/*

// tests that bootnodes are dialed if no peers are connectd, but not otherwise.
func TestDialStateDynDialBootnode(t *testing.T) {
	config := &Config{
		BootstrapNodes: []*enode.Node{
			newNode(uintID(1), nil),
			newNode(uintID(2), nil),
			newNode(uintID(3), nil),
		},
		Logger: testlog.Logger(t, log.LvlTrace),
	}
	runDialTest(t, dialtest{
		init: newDialState(enode.ID{}, 5, config),
		rounds: []round{
			{
				new: []task{
					&discoverTask{want: 5},
				},
			},
			{
				done: []task{
					&discoverTask{
						results: []*enode.Node{
							newNode(uintID(4), nil),
							newNode(uintID(5), nil),
						},
					},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(4), nil)},
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(5), nil)},
					&discoverTask{want: 3},
				},
			},
			// No dials succeed, bootnodes still pending fallback interval
			{},
			// 1 bootnode attempted as fallback interval was reached
			{
				done: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(4), nil)},
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(5), nil)},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(1), nil)},
				},
			},
			// No dials succeed, 2nd bootnode is attempted
			{
				done: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(1), nil)},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(2), nil)},
				},
			},
			// No dials succeed, 3rd bootnode is attempted
			{
				done: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(2), nil)},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(3), nil)},
				},
			},
			// No dials succeed, 1st bootnode is attempted again, expired random nodes retried
			{
				done: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(3), nil)},
					&discoverTask{results: []*enode.Node{
						newNode(uintID(6), nil),
					}},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: newNode(uintID(6), nil)},
					&discoverTask{want: 4},
				},
			},
			// Random dial succeeds, no more bootnodes are attempted
			{
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(6), nil)}},
				},
			},
		},
	})
}

// // This test checks that candidates that do not match the netrestrict list are not dialed.
func TestDialStateNetRestrict(t *testing.T) {
	// This table always returns the same random nodes
	// in the order given below.
	nodes := []*enode.Node{
		newNode(uintID(1), net.ParseIP("127.0.0.1")),
		newNode(uintID(2), net.ParseIP("127.0.0.2")),
		newNode(uintID(3), net.ParseIP("127.0.0.3")),
		newNode(uintID(4), net.ParseIP("127.0.0.4")),
		newNode(uintID(5), net.ParseIP("127.0.2.5")),
		newNode(uintID(6), net.ParseIP("127.0.2.6")),
		newNode(uintID(7), net.ParseIP("127.0.2.7")),
		newNode(uintID(8), net.ParseIP("127.0.2.8")),
	}
	restrict := new(netutil.Netlist)
	restrict.Add("127.0.2.0/24")

	runDialTest(t, dialtest{
		init: newDialState(enode.ID{}, 10, &Config{NetRestrict: restrict}),
		rounds: []round{
			{
				new: []task{
					&discoverTask{want: 10},
				},
			},
			{
				done: []task{
					&discoverTask{results: nodes},
				},
				new: []task{
					&dialTask{flags: dynDialedConn, dest: nodes[4]},
					&dialTask{flags: dynDialedConn, dest: nodes[5]},
					&dialTask{flags: dynDialedConn, dest: nodes[6]},
					&dialTask{flags: dynDialedConn, dest: nodes[7]},
					&discoverTask{want: 6},
				},
			},
		},
	})
}

// This test checks that static dials are launched.
func TestDialStateStaticDial(t *testing.T) {
	config := &Config{
		StaticNodes: []*enode.Node{
			newNode(uintID(1), nil),
			newNode(uintID(2), nil),
			newNode(uintID(3), nil),
			newNode(uintID(4), nil),
			newNode(uintID(5), nil),
		},
		Logger: testlog.Logger(t, log.LvlTrace),
	}
	runDialTest(t, dialtest{
		init: newDialState(enode.ID{}, 0, config),
		rounds: []round{
			// Static dials are launched for the nodes that
			// aren't yet connected.
			{
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
				},
				new: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(3), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(4), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(5), nil)},
				},
			},
			// No new tasks are launched in this round because all static
			// nodes are either connected or still being dialed.
			{
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), nil)}},
				},
				done: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(3), nil)},
				},
			},
			// No new dial tasks are launched because all static
			// nodes are now connected.
			{
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(4), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(5), nil)}},
				},
				done: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(4), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(5), nil)},
				},
				new: []task{
					&waitExpireTask{Duration: 19 * time.Second},
				},
			},
			// Wait a round for dial history to expire, no new tasks should spawn.
			{
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(2), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(4), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(5), nil)}},
				},
			},
			// If a static node is dropped, it should be immediately redialed,
			// irrespective whether it was originally static or dynamic.
			{
				done: []task{
					&waitExpireTask{Duration: 19 * time.Second},
				},
				peers: []*Peer{
					{rw: &conn{flags: dynDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(3), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(5), nil)}},
				},
				new: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(2), nil)},
				},
			},
		},
	})
}

// This test checks that past dials are not retried for some time.
func TestDialStateCache(t *testing.T) {
	config := &Config{
		StaticNodes: []*enode.Node{
			newNode(uintID(1), nil),
			newNode(uintID(2), nil),
			newNode(uintID(3), nil),
		},
		Logger: testlog.Logger(t, log.LvlTrace),
	}
	runDialTest(t, dialtest{
		init: newDialState(enode.ID{}, 0, config),
		rounds: []round{
			// Static dials are launched for the nodes that
			// aren't yet connected.
			{
				peers: nil,
				new: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(1), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(2), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(3), nil)},
				},
			},
			// No new tasks are launched in this round because all static
			// nodes are either connected or still being dialed.
			{
				peers: []*Peer{
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), nil)}},
				},
				done: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(1), nil)},
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(2), nil)},
				},
			},
			// A salvage task is launched to wait for node 3's history
			// entry to expire.
			{
				peers: []*Peer{
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), nil)}},
				},
				done: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(3), nil)},
				},
				new: []task{
					&waitExpireTask{Duration: 19 * time.Second},
				},
			},
			// Still waiting for node 3's entry to expire in the cache.
			{
				peers: []*Peer{
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), nil)}},
				},
			},
			{
				peers: []*Peer{
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), nil)}},
				},
			},
			// The cache entry for node 3 has expired and is retried.
			{
				done: []task{
					&waitExpireTask{Duration: 19 * time.Second},
				},
				peers: []*Peer{
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(1), nil)}},
					{rw: &conn{flags: staticDialedConn, node: newNode(uintID(2), nil)}},
				},
				new: []task{
					&dialTask{flags: staticDialedConn, dest: newNode(uintID(3), nil)},
				},
			},
		},
	})
}

func TestDialResolve(t *testing.T) {
	config := &Config{
		Logger: testlog.Logger(t, log.LvlTrace),
		Dialer: TCPDialer{&net.Dialer{Deadline: time.Now().Add(-5 * time.Minute)}},
	}
	resolved := newNode(uintID(1), net.IP{127, 0, 55, 234})
	resolver := &resolveMock{answer: resolved}
	state := newDialState(enode.ID{}, 0, config)

	// Check that the task is generated with an incomplete ID.
	dest := newNode(uintID(1), nil)
	state.addStatic(dest)
	tasks := state.newTasks(0, nil, time.Time{})
	if !reflect.DeepEqual(tasks, []task{&dialTask{flags: staticDialedConn, dest: dest}}) {
		t.Fatalf("expected dial task, got %#v", tasks)
	}

	// Now run the task, it should resolve the ID once.
	srv := &Server{
		Config:             *config,
		log:                config.Logger,
		staticNodeResolver: resolver,
	}
	tasks[0].Do(srv)
	if !reflect.DeepEqual(resolver.calls, []*enode.Node{dest}) {
		t.Fatalf("wrong resolve calls, got %v", resolver.calls)
	}

	// Report it as done to the dialer, which should update the static node record.
	state.taskDone(tasks[0], time.Now())
	if state.static[uintID(1)].dest != resolved {
		t.Fatalf("state.dest not updated")
	}
}

*/

type dialTestRound struct {
	peers        []*Peer       // current peer set
	discovered   []*enode.Node // newly discovered nodes
	completed    []*enode.Node // dials that complete in this round
	wantNewDials []*enode.Node // dials that should be launched in this round
}

func runDialTest(t *testing.T, rounds []dialTestRound) {
	pm := func(ps []*Peer) map[enode.ID]*Peer {
		m := make(map[enode.ID]*Peer)
		for _, p := range ps {
			m[p.ID()] = p
		}
		return m
	}

	var (
		clock     = new(mclock.Simulated)
		iterator  = newDialTestIterator()
		dialer    = newDialTestDialer()
		logger    = testlog.Logger(t, log.LvlTrace)
		limit     = 20
		config    = Config{clock: clock, Logger: logger, Dialer: dialer}
		server    = &Server{Config: config}
		dialstate = newDialer2(server, limit, iterator)
	)
	defer dialstate.stop()

	for i, round := range rounds {
		dialstate.setPeers(pm(round.peers))
		iterator.addNodes(round.discovered)

		if err := dialer.completeDials(round.completed); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if err := dialer.waitForDials(round.wantNewDials); err != nil {
			t.Fatalf("round %d: %v", i, err)
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

	timeout := time.NewTimer(3 * time.Second)
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
	return nil
}

// completeDials unblocks calls to Dial for the given nodes.
func (d *dialTestDialer) completeDials(nodes []*enode.Node) error {
	for _, n := range nodes {
		req := d.blocked[n.ID()]
		if req == nil {
			return fmt.Errorf("can't complete dial to %v", n.ID())
		}
		req.unblock <- nil
	}
	return nil
}

func uintID(i uint32) enode.ID {
	var id enode.ID
	binary.BigEndian.PutUint32(id[:], i)
	return id
}

// for TestDialResolve
type resolveMock struct {
	calls  []*enode.Node
	answer *enode.Node
}

func (t *resolveMock) Resolve(n *enode.Node) *enode.Node {
	t.calls = append(t.calls, n)
	return t.answer
}
