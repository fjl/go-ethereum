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
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// topicSystem manages the resources required for registering and searching
// in topics.
type topicSystem struct {
	transport *UDPv5
	config    topicindex.Config

	mu     sync.Mutex
	reg    map[topicindex.TopicID]*topicReg
	search map[topicindex.TopicID]*topicSearch
}

func newTopicSystem(transport *UDPv5, config topicindex.Config) *topicSystem {
	return &topicSystem{
		transport: transport,
		config:    config,
		reg:       make(map[topicindex.TopicID]*topicReg),
		search:    make(map[topicindex.TopicID]*topicSearch),
	}
}

func (sys *topicSystem) register(topic topicindex.TopicID, opid uint64) {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	if _, ok := sys.reg[topic]; ok {
		return
	}
	sys.reg[topic] = newTopicReg(sys, topic, opid)
}

func (sys *topicSystem) stopRegister(topic topicindex.TopicID) {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	if reg := sys.reg[topic]; reg != nil {
		reg.stop()
		delete(sys.reg, topic)
	}
}

func (sys *topicSystem) stop() {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	for topic, reg := range sys.reg {
		reg.stop()
		delete(sys.reg, topic)
	}
	for topic, s := range sys.search {
		s.stop()
		delete(sys.search, topic)
	}
}

func (sys *topicSystem) newSearchIterator(topic topicindex.TopicID, opid uint64) enode.Iterator {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	resultCh := make(chan *enode.Node, 200)
	s := newTopicSearch(sys, topic, resultCh, opid)
	return newTopicSearchIterator(sys, s, resultCh)
}

// topicReg handles registering for a single topic.
type topicReg struct {
	state *topicindex.Registration
	clock mclock.Clock
	opid  uint64

	wg   sync.WaitGroup
	quit chan struct{}

	regRequest  chan *topicindex.RegAttempt
	regResponse chan topicRegResult

	// nodes subscription
	newNodesCh  chan *enode.Node
	newNodesSub event.Subscription
}

func newTopicReg(sys *topicSystem, topic topicindex.TopicID, opid uint64) *topicReg {
	reg := &topicReg{
		state:       topicindex.NewRegistration(topic, sys.config),
		clock:       sys.config.Clock,
		opid:        opid,
		quit:        make(chan struct{}),
		regRequest:  make(chan *topicindex.RegAttempt),
		regResponse: make(chan topicRegResult),
	}

	// Set up the subscription for new main table nodes.
	reg.newNodesCh = make(chan *enode.Node, 100)
	reg.newNodesSub = sys.transport.tab.subscribeNodes(reg.newNodesCh)

	reg.wg.Add(2)
	go reg.run(sys)
	go reg.runRequests(sys)
	return reg
}

func (reg *topicReg) stop() {
	close(reg.quit)
	reg.wg.Wait()
}

func (reg *topicReg) run(sys *topicSystem) {
	defer reg.wg.Done()
	defer reg.newNodesSub.Unsubscribe()
	defer close(reg.regRequest)

	time := mclock.AbsTime(-1)
	for {
		if time >= 0 {
			if exit := reg.pause(time); exit {
				return
			}
		}
		time = reg.clock.Now()

		// Initialize the registration state.
		nodes := sys.transport.tab.Nodes()
		if len(nodes) == 0 {
			continue // Local table is empty, retry later.
		}
		reg.state.AddNodes(nil, nodes)

		// Perform registration.
		if exit := reg.runRegistration(sys); exit {
			return
		}
	}
}

const regloopMinTime = 2 * time.Second

// pause ensures that top-level registration loop iterations take at least regLoopMinTime.
// This prevents the loop from running too hot when the local node table is very empty.
func (reg *topicReg) pause(lastTime mclock.AbsTime) bool {
	d := reg.clock.Now().Sub(lastTime)
	if d < regloopMinTime {
		sleep := reg.clock.NewTimer(regloopMinTime - d)
		defer sleep.Stop()
		for {
			select {
			case <-sleep.C():
				return false
			case <-reg.newNodesCh:
				// Need to read this channel to avoid blocking in Table.
			case <-reg.quit:
				return true
			}
		}
	}
	return false
}

func (reg *topicReg) runRegistration(sys *topicSystem) (exit bool) {
	var (
		updateEv      = mclock.NewAlarm(reg.clock)
		updateCh      <-chan struct{}
		sendAttempt   *topicindex.RegAttempt
		sendAttemptCh chan<- *topicindex.RegAttempt
	)

	for {
		if reg.state.NodeCount() == 0 {
			// State ran out of nodes, re-initialize.
			return false
		}

		// Disable updates while dispatching the next attempt's request.
		if sendAttempt == nil {
			next := reg.state.NextUpdateTime()
			if next != topicindex.Never {
				updateEv.Schedule(next)
				updateCh = updateEv.C()
			}
		}

		select {
		// Loop exit.
		case <-reg.quit:
			return true

		case n := <-reg.newNodesCh:
			reg.state.AddNodes(nil, []*enode.Node{n})

		// Attempt queue updates.
		case <-updateCh:
			updateCh = nil
			att := reg.state.Update()
			if att != nil {
				sendAttempt = att
				sendAttemptCh = reg.regRequest
			}

		// Registration requests.
		case sendAttemptCh <- sendAttempt:
			reg.state.StartRequest(sendAttempt)
			sendAttempt, sendAttemptCh = nil, nil

		case resp := <-reg.regResponse:
			if resp.err != nil {
				reg.state.HandleErrorResponse(resp.att, resp.err)
				continue
			}

			// TODO: handle overflow
			wt := time.Duration(resp.msg.WaitTime) * time.Millisecond

			if len(resp.msg.Ticket) > 0 {
				reg.state.HandleTicketResponse(resp.att, resp.msg.Ticket, wt)
			} else {
				// No ticket - registration successful. WaitTime field means ad lifetime.
				reg.state.HandleRegistered(resp.att, wt)
			}

			reg.state.AddNodes(resp.att.Node, resp.nodes)
		}
	}
}

type topicRegResult struct {
	msg   *v5wire.Regconfirmation
	nodes []*enode.Node
	err   error

	att *topicindex.RegAttempt
}

// runRequests performs topic registration requests.
// TODO: this is not great because it sends one at a time and waits for a response.
// registrations could be sent more async.
func (reg *topicReg) runRequests(sys *topicSystem) {
	defer reg.wg.Done()

	for attempt := range reg.regRequest {
		n := attempt.Node
		topic := reg.state.Topic()
		resp := sys.transport.regtopic(n, topic, attempt.Ticket, reg.opid)
		resp.att = attempt

		// Send response to main loop.
		select {
		case reg.regResponse <- resp:
		case <-reg.quit:
			return
		}
	}
}

// topicSearch handles searching in a single topic.
type topicSearch struct {
	topic  topicindex.TopicID
	opid   uint64
	config topicindex.Config

	wg   sync.WaitGroup
	quit chan struct{}

	queryCh     chan *enode.Node
	queryRespCh chan topicQueryResp

	resultCh chan *enode.Node

	lookupCtx     context.Context
	lookupCancel  context.CancelFunc
	lookupCh      chan enode.ID
	lookupResults chan []*enode.Node
}

type topicQueryResp struct {
	src   *enode.Node
	nodes []*enode.Node
	err   error
}

func newTopicSearch(sys *topicSystem, topic topicindex.TopicID, out chan *enode.Node, opid uint64) *topicSearch {
	ctx, cancel := context.WithCancel(context.Background())

	s := &topicSearch{
		topic:    topic,
		config:   sys.config,
		opid:     opid,
		quit:     make(chan struct{}),
		resultCh: out,

		// query
		queryCh:     make(chan *enode.Node),
		queryRespCh: make(chan topicQueryResp),

		// lookup
		lookupCtx:     ctx,
		lookupCancel:  cancel,
		lookupCh:      make(chan enode.ID),
		lookupResults: make(chan []*enode.Node, 1),
	}
	s.wg.Add(3)
	go s.run()
	go s.runLookups(sys)
	go s.runRequests(sys)
	return s
}

func (s *topicSearch) stop() {
	close(s.quit)
	s.wg.Wait()
}

func (s *topicSearch) run() {
	defer s.wg.Done()

	var (
		state        = topicindex.NewSearch(s.topic, s.config)
		lookupEv     = mclock.NewAlarm(s.config.Clock)
		lookupCh     chan enode.ID
		lookupTarget enode.ID
		queryCh      chan<- *enode.Node
		queryTarget  *enode.Node
		resultCh     chan<- *enode.Node
		result       *enode.Node
		nresults     int
	)

	for {
		// State rollover.
		if state.IsDone() {
			s.config.Log.Debug("Topic search rollover", "topic", s.topic, "nres", nresults)
			state = topicindex.NewSearch(s.topic, s.config)
			nresults = 0
		}
		// Schedule lookups.
		if lookupCh == nil {
			next := state.NextLookupTime()
			if next != topicindex.Never {
				lookupEv.Schedule(next)
			}
		}
		// Ensure there is always one query running.
		if queryTarget == nil {
			t := state.QueryTarget()
			if t != nil {
				queryCh = s.queryCh
				queryTarget = t
			}
		}
		// Dispatch result when available.
		if n := state.PeekResult(); n != nil {
			result = n
			resultCh = s.resultCh
		}

		select {
		// Loop exit.
		case <-s.quit:
			s.lookupCancel()
			close(s.lookupCh)
			close(s.queryCh)
			// Drain result channel. This guarantees that, when the iterator's
			// Close is done, Next will always return false.
			close(s.resultCh)
			for range s.resultCh {
			}
			return

		// Lookup management.
		case <-lookupEv.C():
			lookupTarget = state.LookupTarget()
			lookupCh = s.lookupCh

		case lookupCh <- lookupTarget:
		case nodes := <-s.lookupResults:
			state.AddLookupNodes(nodes)
			lookupCh = nil

		// Queries.
		case queryCh <- queryTarget:
		case resp := <-s.queryRespCh:
			if resp.err != nil {
				s.config.Log.Debug("TOPICQUERY failed", "topic", s.topic, "id", resp.src.ID(), "err", resp.err)
			}
			state.AddQueryResults(resp.src, resp.nodes)
			queryTarget, queryCh = nil, nil

		// Results.
		case resultCh <- result:
			nresults++
			state.PopResult()
			result, resultCh = nil, nil
		}
	}
}

// TODO: this should be shared with topicReg somehow.
func (s *topicSearch) runLookups(sys *topicSystem) {
	defer s.wg.Done()

	for target := range s.lookupCh {
		l := sys.transport.newLookup(s.lookupCtx, target, s.opid)
		// Note: here, only the final results (i.e. closest to target) are taken.
		nodes := l.run()
		select {
		case s.lookupResults <- nodes:
		case <-s.lookupCtx.Done():
			return
		}
	}
}

func (s *topicSearch) runRequests(sys *topicSystem) {
	defer s.wg.Done()

	for n := range s.queryCh {
		resp := topicQueryResp{src: n}
		resp.nodes, resp.err = sys.transport.topicQuery(n, s.topic, s.opid)

		// Send response to main loop.
		select {
		case s.queryRespCh <- resp:
		case <-s.quit:
			return
		}
	}
}

// topicSearchIterator implements enode.Iterator. It is an iterator
// that returns nodes found by topic search.
type topicSearchIterator struct {
	sys     *topicSystem
	search  *topicSearch
	ch      <-chan *enode.Node
	closing sync.Once
	cur     *enode.Node
}

func newTopicSearchIterator(sys *topicSystem, search *topicSearch, ch <-chan *enode.Node) *topicSearchIterator {
	return &topicSearchIterator{sys: sys, search: search, ch: ch}
}

func (tsi *topicSearchIterator) Next() bool {
	n, ok := <-tsi.ch
	tsi.cur = n
	return ok
}

func (tsi *topicSearchIterator) Node() *enode.Node {
	return tsi.cur
}

func (tsi *topicSearchIterator) Close() {
	tsi.closing.Do(tsi.search.stop)
}
