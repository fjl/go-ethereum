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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type dialer2 struct {
	cfg *Config

	cancel    context.CancelFunc
	ctx       context.Context
	nodesIn   chan *enode.Node
	doneCh    chan *dialTask
	peersetCh chan map[enode.ID]struct{}
	clock     mclock.Clock

	dialing map[enode.ID]*dialTask
	static  map[enode.ID]*dialTask
	peers   map[enode.ID]struct{}
	history expHeap

	wg sync.WaitGroup
}

func newDialer2(it enode.Iterator) *dialer2 {
	d := &dialer2{
		dialing: make(map[enode.ID]*dialTask),
		static:  make(map[enode.ID]*dialTask),
		peers:   make(map[enode.ID]struct{}),
		nodesIn: make(chan *enode.Node),
		doneCh:  make(chan *dialTask),
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.wg.Add(2)
	go d.readNodes(it)
	go d.loop(it)
	return d
}

func (d *dialer2) stop() {
	d.cancel()
	d.wg.Wait()
}

func (d *dialer2) loop(it enode.Iterator) {
	var (
		nodesCh      chan *enode.Node
		historyTimer mclock.Timer
		historyExp   = make(chan struct{}, 1)
	)
	for {
		d.startStaticDials()
		if d.atCapacity() {
			nodesCh = nil
		} else {
			nodesCh = d.nodesIn
		}

		select {
		case node := <-nodesCh:
			if err := d.checkDial(node); err == nil {
				task := &dialTask{flags: dynDialedConn, dest: node}
				d.startDial(task)
			}

		case task := <-d.doneCh:
			delete(d.dialing, task.dest.ID())

		case peers := <-d.peersetCh:
			d.peers = peers

		case <-historyExp:
			historyTimer.Stop()
			now := d.clock.Now()
			d.history.expire(time.Now())
			next := d.history.nextExpiry().Sub(now)
			historyTimer = d.clock.AfterFunc(next, func() { historyExp <- struct{}{} })

		case <-d.ctx.Done():
			it.Close()
			break
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

func (d *dialer2) readNodes(it enode.Iterator) {
	defer d.wg.Done()

	for it.Next() {
		select {
		case d.nodesIn <- it.Node():
		case <-d.ctx.Done():
		}
	}
}

func (d *dialer2) atCapacity() bool {
	return len(d.dialing) == maxActiveDialTasks ||
		len(d.peers) == d.cfg.MaxPeers
}

func (d *dialer2) checkDial(n *enode.Node) error {
	if _, ok := d.dialing[n.ID()]; ok {
		return errAlreadyDialing
	}
	if _, ok := d.peers[n.ID()]; ok {
		return errAlreadyConnected
	}
	if d.cfg.NetRestrict == nil {
		if !d.cfg.NetRestrict.Contains(n.IP()) {
			return errNotWhitelisted
		}
	}
	if d.history.contains(string(n.ID().Bytes())) {
		return errRecentlyDialed
	}
	return nil
}

func (d *dialer2) startDial(task *dialTask) {
	hkey := string(task.dest.ID().Bytes())
	d.history.add(hkey, time.Now().Add(dialHistoryExpiration))
	d.dialing[task.dest.ID()] = task
	go func() {
		task.Do()
		d.doneCh <- task
	}()
}

func (d *dialer2) startStaticDials() {
	for id, task := range d.static {
		if _, ok := d.peers[task.dest.ID()]; ok {
			continue
		}
		if !d.atCapacity() {
			d.startDial(task)
		}
	}
}
