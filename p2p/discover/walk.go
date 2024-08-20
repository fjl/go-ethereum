// Copyright 2024 The go-ethereum Authors
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
	"context"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// walk implements a process that navigates the DHT.
type walk struct {
	tab         *Table
	seen        map[enode.ID]bool
	replyBuffer []*enode.Node
	route       route
	queries     int
	inited      bool
}

type query struct {
	target any
	node   *enode.Node
	// response fields
	err  error
	resp []*enode.Node
}

type route interface {
	init(*Table) bool
	nextHop() (*enode.Node, any)
	addFoundNodes([]*enode.Node)
	result() []*enode.Node
}

func newWalk(tab *Table, r route) *walk {
	w := &walk{
		tab:   tab,
		seen:  make(map[enode.ID]bool),
		route: r,
	}
	w.seen[tab.self().ID()] = true
	return w
}

func (w *walk) advance() (q *query, done bool) {
	if !w.inited {
		if !w.route.init(w.tab) {
			return nil, true
		}
		w.inited = true
	}
	if w.queries >= alpha {
		return nil, false
	}
	n, target := w.route.nextHop()
	if n != nil {
		q = &query{node: n, target: target}
		w.queries++
	}
	return q, w.queries == 0
}

func (w *walk) handleResponse(q *query) {
	w.replyBuffer = w.replyBuffer[:0]
	for _, n := range q.resp {
		if n != nil && !w.seen[n.ID()] {
			w.seen[n.ID()] = true
			w.replyBuffer = append(w.replyBuffer, n)
		}
	}
	w.route.addFoundNodes(w.replyBuffer)
	w.queries--

	// XXX: how do we quantify success? The flag here signifies whether the node is considered
	// live by Table. Here we use len(q.resp) > 0 because it means the node returned *something*
	// which indicates it is participating in some way.
	// Perhaps q.err could also be used.
	success := len(q.resp) > 0
	w.tab.trackRequest(q.node, success, w.replyBuffer)
}

type walkTransport interface {
	runLookupQuery(context.Context, *query)
}

// runWalk steps through a walk and returns the result nodes collected by the route.
func runWalk(ctx context.Context, transport walkTransport, w *walk) []*enode.Node {
	var (
		replyCh  = make(chan *query, alpha)
		shutdown bool
	)
	for {
		q, done := w.advance()
		if done {
			return w.route.result()
		}
		if q != nil && !shutdown {
			go func() {
				transport.runLookupQuery(ctx, q)
				replyCh <- q
			}()
		}
		select {
		case q := <-replyCh:
			w.handleResponse(q)
		case <-ctx.Done():
			shutdown = true
		}
	}
}

// walkIterator steps through a walk, returning all visited nodes.
type walkIterator struct {
	node      *enode.Node // the current node of the iterator
	w         *walk
	transport walkTransport
	respCh    chan *query
	closeCtx  context.Context
	doClose   context.CancelFunc
}

func newWalkIterator(tab *Table, r route) *walkIterator {
	ctx, cancel := context.WithCancel(context.Background())
	return &walkIterator{
		w:         newWalk(tab, r),
		transport: tab.net,
		respCh:    make(chan *query, alpha),
		closeCtx:  ctx,
		doClose:   cancel,
	}
}

func (wit *walkIterator) Next() bool {
	// Check if closed.
	select {
	case <-wit.closeCtx.Done():
		wit.node = nil
		return false
	default:
	}

	// Go to next node.
	for {
		q, done := wit.w.advance()
		if done {
			wit.node = nil
			wit.Close()
			return false
		}
		if q != nil {
			wit.node = q.node
			go func() {
				wit.transport.runLookupQuery(context.Background(), q)
				wit.respCh <- q
			}()
			return true
		}
		// Need to wait for a response to proceed.
		select {
		case <-wit.closeCtx.Done():
			wit.node = nil
			return false
		case q := <-wit.respCh:
			wit.w.handleResponse(q)
		}
	}
}

func (wit *walkIterator) Node() *enode.Node {
	return wit.node
}

func (wit *walkIterator) Close() {
	wit.doClose()
	// TODO: wait for queries to end
}
