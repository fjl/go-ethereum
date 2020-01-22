// Copyright 2019 The go-ethereum Authors
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
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// checkDial errors:
var (
	errSelf             = errors.New("is self")
	errAlreadyDialing   = errors.New("already dialing")
	errAlreadyConnected = errors.New("already connected")
	errRecentlyDialed   = errors.New("recently dialed")
	errNotWhitelisted   = errors.New("not contained in netrestrict whitelist")
)

// dialer creates outbound connections and submits them into Server.
// Two types of peer connections can be created:
//
//  - static dials are pre-configured connections. The dialer attempts
//    keep these nodes connected at all times.
//
//  - dynamic dials are created from node discovery results. The dialer
//    continuously reads candidate nodes from its input iterator and attempts
//    to create peer connections to nodes arriving through the iterator.
//
type dialer2 struct {
	dialerConfig
	setupFunc dialerSetupFunc

	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	nodesIn     chan *enode.Node
	doneCh      chan *dialTask2
	peersetCh   chan map[enode.ID]*Peer
	addStaticCh chan *enode.Node
	remStaticCh chan *enode.Node
	addPeerCh chan *conn
	remPeerCh chan *conn


	// State of loop.
	dialing       map[enode.ID]*dialTask2
	peers         map[enode.ID]connFlag
	history       expHeap
	dialPeerCount int

	static         map[enode.ID]*dialTask2
	staticTaskPool []*dialTask2
}

type dialerConfig struct {
	self           enode.ID
	maxDialPeers   int              // maximum number of connected dyn-dialed peers
	maxActiveDials int              // maximum number of active dials
	netRestrict    *netutil.Netlist // IP whitelist, disabled if nil
	resolver       nodeResolver
	dialer         NodeDialer
	log            log.Logger
	clock          mclock.Clock
	rand           *rand.Rand
}

type dialerSetupFunc func(net.Conn, connFlag, *enode.Node) error

func (cfg dialerConfig) withDefaults() dialerConfig {
	if cfg.log == nil {
		cfg.log = log.Root()
	}
	if cfg.clock == nil {
		cfg.clock = mclock.System{}
	}
	if cfg.rand == nil {
		cfg.rand = rand.New(rand.NewSource(0x33))
	}
	return cfg
}

func newDialer2(config dialerConfig, it enode.Iterator, setupFunc dialerSetupFunc) *dialer2 {
	d := &dialer2{
		dialerConfig: config.withDefaults(),
		setupFunc:    setupFunc,
		dialing:      make(map[enode.ID]*dialTask2),
		static:       make(map[enode.ID]*dialTask2),
		peers:        make(map[enode.ID]connFlag),
		doneCh:       make(chan *dialTask2),
		nodesIn:      make(chan *enode.Node),
		addStaticCh:  make(chan *enode.Node),
		remStaticCh:  make(chan *enode.Node),
		addPeerCh:    make(chan *conn),
		remPeerCh:    make(chan *conn),
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())

	d.wg.Add(2)
	go d.readNodes(it)
	go d.loop(it)
	return d
}

// stop shuts down the dialer, canceling all current dial tasks.
func (d *dialer2) stop() {
	d.cancel()
	d.wg.Wait()
}

// addStatic adds a static dial candidate.
func (d *dialer2) addStatic(n *enode.Node) {
	select {
	case d.addStaticCh <- n:
	case <-d.ctx.Done():
	}
}

// removeStatic removes a static dial candidate.
func (d *dialer2) removeStatic(n *enode.Node) {
	select {
	case d.remStaticCh <- n:
	case <-d.ctx.Done():
	}
}

// peerAdded updates the peer set.
func (d *dialer2) peerAdded(c *conn) {
	select {
	case d.addPeerCh <- c:
	case <-d.ctx.Done():
	}
}

// peerRemoved updates the peer set.
func (d *dialer2) peerRemoved(c *conn) {
	select {
	case d.remPeerCh <- c:
	case <-d.ctx.Done():
	}
}

// loop is the main loop of the dialer.
func (d *dialer2) loop(it enode.Iterator) {
	var (
		nodesCh      chan *enode.Node
		historyTimer mclock.Timer
		historyExp   = make(chan struct{}, 1)
	)
loop:
	for {
		// Launch new dials if slots are available.
		slots := d.numDialSlots()
		slots -= d.startStaticDials(slots)
		if slots > 0 {
			nodesCh = d.nodesIn
		} else {
			nodesCh = nil
		}
		// Rearm history timer.
		if historyTimer == nil && len(d.history) > 0 {
			next := time.Duration(d.history.nextExpiry() - d.clock.Now())
			historyTimer = d.clock.AfterFunc(next, func() { historyExp <- struct{}{} })
		}
		
		select {
		case node := <-nodesCh:
			d.log.Trace("Node found", "id", node.ID())
			if err := d.checkDial(node); err != nil {
				d.log.Trace("Discarding dial candidate", "id", node.ID(), "ip", node.IP(), "err", err)
			} else {
				task := &dialTask2{flags: dynDialedConn, dest: node}
				d.startDial(task)
			}

		case task := <-d.doneCh:
			d.log.Trace("Dial done", "id", task.dest.ID())
			delete(d.dialing, task.dest.ID())
			if task.flags&staticDialedConn != 0 {
				d.updateStaticPool(task.dest.ID())
			}

		case c := <-d.addPeerCh:
			d.log.Trace("Peer added", "id", c.node.ID(), "p", len(d.peers) + 1)
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeerCount++
			}
			d.peers[c.node.ID()] = c.flags
			// TODO: cancel dials to connected peers

		case c := <-d.remPeerCh:
			d.log.Trace("Peer removed", "id", c.node.ID(), "p", len(d.peers) - 1)
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeerCount--
			}
			delete(d.peers, c.node.ID())
			d.updateStaticPool(c.node.ID())
			
		case node := <-d.addStaticCh:
			id := node.ID()
			d.log.Trace("Adding static node", "id", id, "ip", node.IP())
			if d.static[id] != nil {
				continue
			}
			task := &dialTask2{dest: node, flags: staticDialedConn}
			d.static[id] = task
			if d.checkDial(node) == nil {
				d.addToStaticPool(task)
			}

		case node := <-d.remStaticCh:
			id := node.ID()
			d.log.Trace("Removing static node", "id", id, "ip", node.IP())
			if d.static[id] == nil {
				continue
			}
			delete(d.static, id)
			for i := range d.staticTaskPool {
				if d.staticTaskPool[i].dest.ID() == id {
					d.removeFromStaticPool(i)
					break
				}
			}

		case <-historyExp:
			d.log.Trace("Dial history expire")
			historyTimer.Stop()
			historyTimer = nil
			d.history.expire(d.clock.Now(), func (hkey string) {
				var id enode.ID
				copy(id[:], hkey)
				d.updateStaticPool(id)
			})

		case <-d.ctx.Done():
			it.Close()
			break loop
		}
	}

	if historyTimer != nil {
		historyTimer.Stop()
	}
	for range d.dialing {
		<-d.doneCh
	}
	d.wg.Done()
}

// readNodes runs in its own goroutine and delivers nodes from
// the input iterator to the nodesIn channel.
func (d *dialer2) readNodes(it enode.Iterator) {
	defer d.wg.Done()

	for it.Next() {
		select {
		case d.nodesIn <- it.Node():
		case <-d.ctx.Done():
		}
	}
}

// numDialSlots returns the number of free dial slots.
func (d *dialer2) numDialSlots() int {
	slots := (d.maxDialPeers - d.dialPeerCount) * 2
	if slots > d.maxActiveDials {
		slots = d.maxActiveDials
	}
	free := slots - len(d.dialing)
	d.log.Trace("Dial slot stats", "free", free, "total", slots, "p", d.dialPeerCount, "maxp", d.maxDialPeers, "pool", len(d.staticTaskPool))
	return free
}

func (d *dialer2) updatePeerInfo(peers map[enode.ID]*Peer) {
	d.dialPeerCount = 0
	// Count up dial peers.
	// TODO cancel in-progress dials.
	for _, p := range peers {
		if p.rw.is(dynDialedConn) || p.rw.is(staticDialedConn) {
			d.dialPeerCount++
		}
	}
}

// checkDial returns an error if node n should not be dialed.
func (d *dialer2) checkDial(n *enode.Node) error {
	if _, ok := d.dialing[n.ID()]; ok {
		return errAlreadyDialing
	}
	if _, ok := d.peers[n.ID()]; ok {
		return errAlreadyConnected
	}
	if d.netRestrict != nil && !d.netRestrict.Contains(n.IP()) {
		return errNotWhitelisted
	}
	if d.history.contains(string(n.ID().Bytes())) {
		return errRecentlyDialed
	}
	return nil
}

// startStaticDials starts all configured static dial tasks.
func (d *dialer2) startStaticDials(n int) (started int) {
	for started = 0; started < n && len(d.staticTaskPool) > 0; started++ {
		idx := d.rand.Intn(len(d.staticTaskPool))
		task := d.staticTaskPool[idx]
		d.startDial(task)
		d.removeFromStaticPool(idx)
	}
	return started
}

// updateStaticPool attempts to move the given static dial back into
// d.staticTaskPool.
func (d *dialer2) updateStaticPool(id enode.ID) {
	task, ok := d.static[id]
	if !ok || task.inStaticPool || d.checkDial(task.dest) != nil {
		return
	}
	d.addToStaticPool(task)
}

func (d *dialer2) addToStaticPool(task *dialTask2) {
	if task.inStaticPool {
		panic("attempt to add task to staticPool twice")
	}
	task.inStaticPool = true
	d.staticTaskPool = append(d.staticTaskPool, task)
}

func (d *dialer2) removeFromStaticPool(idx int) {
	task := d.staticTaskPool[idx]
	if !task.inStaticPool {
		panic("attempt to remove non-existent task from staticPool")
	}
	task.inStaticPool = false
	// Swap with last element, then shorten pool.
	end := len(d.staticTaskPool) - 1
	d.staticTaskPool[end], d.staticTaskPool[idx] = d.staticTaskPool[idx], d.staticTaskPool[end]
	d.staticTaskPool = d.staticTaskPool[:end]
}

// startDial runs the given dial task in a separate goroutine.
func (d *dialer2) startDial(task *dialTask2) {
	d.log.Trace("Starting p2p dial", "id", task.dest.ID(), "ip", task.dest.IP(), "flag", task.flags)
	hkey := string(task.dest.ID().Bytes())
	d.history.add(hkey, d.clock.Now().Add(dialHistoryExpiration))
	d.dialing[task.dest.ID()] = task
	go func() {
		task.run(d)
		d.doneCh <- task
	}()
}

// A dialTask2 is generated for each node that is dialed. Its
// fields cannot be accessed while the task is running.
type dialTask2 struct {
	flags        connFlag
	dest         *enode.Node
	lastResolved mclock.AbsTime
	resolveDelay time.Duration
	inStaticPool bool
}

type dialError struct {
	error
}

func (t *dialTask2) run(d *dialer2) {
	if t.dest.Incomplete() {
		if !t.resolve(d) {
			return
		}
	}

	err := t.dial(d, t.dest)
	if err != nil {
		d.log.Trace("Dial error", "task", t, "err", err)
		// Try resolving the ID of static nodes if dialing failed.
		if _, ok := err.(*dialError); ok && t.flags&staticDialedConn != 0 {
			if t.resolve(d) {
				t.dial(d, t.dest)
			}
		}
	}
}

// resolve attempts to find the current endpoint for the destination
// using discovery.
//
// Resolve operations are throttled with backoff to avoid flooding the
// discovery network with useless queries for nodes that don't exist.
// The backoff delay resets when the node is found.
func (t *dialTask2) resolve(d *dialer2) bool {
	if d.resolver == nil {
		d.log.Debug("Can't resolve node", "id", t.dest.ID(), "err", "discovery is disabled")
		return false
	}
	if t.resolveDelay == 0 {
		t.resolveDelay = initialResolveDelay
	}
	if t.lastResolved > 0 && time.Duration(d.clock.Now()-t.lastResolved) < t.resolveDelay {
		return false
	}
	resolved := d.resolver.Resolve(t.dest)
	t.lastResolved = d.clock.Now()
	if resolved == nil {
		t.resolveDelay *= 2
		if t.resolveDelay > maxResolveDelay {
			t.resolveDelay = maxResolveDelay
		}
		d.log.Debug("Resolving node failed", "id", t.dest.ID(), "newdelay", t.resolveDelay)
		return false
	}
	// The node was found.
	t.resolveDelay = initialResolveDelay
	t.dest = resolved
	d.log.Debug("Resolved node", "id", t.dest.ID(), "addr", &net.TCPAddr{IP: t.dest.IP(), Port: t.dest.TCP()})
	return true
}

// dial performs the actual connection attempt.
func (t *dialTask2) dial(d *dialer2, dest *enode.Node) error {
	fd, err := d.dialer.Dial(d.ctx, dest)
	if err != nil {
		return &dialError{err}
	}
	mfd := newMeteredConn(fd, false, &net.TCPAddr{IP: dest.IP(), Port: dest.TCP()})
	return d.setupFunc(mfd, t.flags, dest)
}

func (t *dialTask2) String() string {
	id := t.dest.ID()
	return fmt.Sprintf("%v %x %v:%d", t.flags, id[:8], t.dest.IP(), t.dest.TCP())
}
