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
	"math/rand"
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

	mu  sync.Mutex
	reg map[topicindex.TopicID]*topicReg
}

func newTopicSystem(transport *UDPv5, config topicindex.Config) *topicSystem {
	return &topicSystem{
		transport: transport,
		config:    config,
		reg:       make(map[topicindex.TopicID]*topicReg),
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

	regRequest  chan topicRegJob
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
		regRequest:  make(chan topicRegJob),
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
		shuffleNodes(nodes)
		reg.state.AddNodes(nil, nodes)

		// Perform registration.
		if exit := reg.runRegistration(sys); exit {
			return
		}
	}
}

func shuffleNodes(nodes []*enode.Node) {
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
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
		nextAttempt   topicRegJob
		sendAttemptCh chan<- topicRegJob
	)

	for {
		if reg.state.NodeCount() == 0 {
			// State ran out of nodes, re-initialize.
			return false
		}

		// Disable updates while dispatching the next attempt's request.
		var updateCh <-chan struct{}
		if sendAttemptCh == nil {
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
			att := reg.state.Update()
			if att != nil {
				sendAttemptCh = reg.regRequest
				nextAttempt = topicRegJob{
					att:     att,
					buckets: reg.state.BucketsWithFreeSpace(regtopicNodesLimit * 2),
				}
			}

		// Registration requests.
		case sendAttemptCh <- nextAttempt:
			reg.state.StartRequest(nextAttempt.att)
			sendAttemptCh = nil

		case resp := <-reg.regResponse:
			if len(resp.nodes) > 0 {
				reg.state.AddNodes(resp.att.Node, resp.nodes)
			}
			if resp.err != nil {
				reg.state.HandleErrorResponse(resp.att, resp.err)
				continue
			}
			// TODO: handle overflow
			wt := time.Duration(resp.msg.WaitTime) * time.Millisecond
			if len(resp.msg.Ticket) > 0 {
				reg.state.HandleTicketResponse(resp.att, resp.msg.Ticket, wt)
			} else {
				// No ticket - registration successful.
				// WaitTime field means ad lifetime.
				reg.state.HandleRegistered(resp.att, wt)
			}
		}
	}
}

type topicRegJob struct {
	att     *topicindex.RegAttempt
	buckets []uint
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

	for job := range reg.regRequest {
		n := job.att.Node
		topic := reg.state.Topic()
		resp := sys.transport.regtopic(n, topic, job.att.Ticket, job.buckets, reg.opid)
		resp.att = job.att

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

	queryCh     chan topicQueryJob
	queryRespCh chan topicQueryResult
	resultCh    chan *enode.Node

	newNodesCh  chan *enode.Node
	newNodesSub event.Subscription
}

func newTopicSearch(sys *topicSystem, topic topicindex.TopicID, out chan *enode.Node, opid uint64) *topicSearch {
	s := &topicSearch{
		topic:    topic,
		config:   sys.config,
		opid:     opid,
		quit:     make(chan struct{}),
		resultCh: out,

		// query
		queryCh:     make(chan topicQueryJob),
		queryRespCh: make(chan topicQueryResult),
	}

	// Set up the subscription for new main table nodes.
	s.newNodesCh = make(chan *enode.Node, 100)
	s.newNodesSub = sys.transport.tab.subscribeNodes(s.newNodesCh)

	s.wg.Add(2)
	go s.runLoop(sys)
	go s.runRequests(sys)
	return s
}

func (s *topicSearch) stop() {
	close(s.quit)
	s.wg.Wait()
}

func (s *topicSearch) runLoop(sys *topicSystem) {
	defer s.wg.Done()
	defer s.newNodesSub.Unsubscribe()
	defer s.closeDown()

	time := mclock.AbsTime(-1)
	for {
		if time >= 0 {
			if exit := s.pause(time); exit {
				return
			}
		}
		time = s.config.Clock.Now()

		state := topicindex.NewSearch(s.topic, s.config)
		nodes := sys.transport.tab.Nodes()
		if len(nodes) == 0 {
			continue // Local table is empty, retry later.
		}
		shuffleNodes(nodes)
		state.AddNodes(nil, nodes)

		if exit := s.run(state); exit {
			return
		}
	}

}

// pause ensures that top-level loop iterations take at least regLoopMinTime.
// This prevents the loop from running too hot when the local node table is very empty.
func (s *topicSearch) pause(lastTime mclock.AbsTime) bool {
	d := s.config.Clock.Now().Sub(lastTime)
	if d < regloopMinTime {
		sleep := s.config.Clock.NewTimer(regloopMinTime - d)
		defer sleep.Stop()
		for {
			select {
			case <-sleep.C():
				return false
			case <-s.newNodesCh:
				// Need to read this channel to avoid blocking in Table.
			case <-s.quit:
				return true
			}
		}
	}
	return false
}

type topicQueryJob struct {
	dst     *enode.Node
	buckets []uint
}

func (s *topicSearch) run(state *topicindex.Search) (exit bool) {
	var (
		queryCh   chan<- topicQueryJob
		nextQuery topicQueryJob
		resultCh  chan<- *enode.Node
		result    *enode.Node
		nresults  int
	)

	for {
		// State rollover.
		if state.IsDone() {
			s.config.Log.Debug("Topic search rollover", "topic", s.topic, "nres", nresults)
			return false
		}
		// Ensure there is always one query running.
		if queryCh == nil {
			target := state.QueryTarget()
			if target != nil {
				queryCh = s.queryCh
				nextQuery = topicQueryJob{
					dst:     target,
					buckets: state.BucketsWithFreeSpace(regtopicNodesLimit * 2),
				}
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
			return true

		// Queries.
		case queryCh <- nextQuery:
		case resp := <-s.queryRespCh:
			state.AddNodes(resp.src, resp.auxNodes)
			state.AddQueryResults(resp.src, resp.topicNodes)
			if resp.err != nil {
				s.config.Log.Debug("TOPICQUERY/v5 failed", "topic", s.topic, "id", resp.src.ID(), "err", resp.err)
			}
			queryCh = nil

		// Results.
		case resultCh <- result:
			nresults++
			state.PopResult()
			result, resultCh = nil, nil
		}
	}
}

func (s *topicSearch) closeDown() {
	close(s.queryCh)
	// Drain result channel. This guarantees that, when the iterator's
	// Close is done, Next will always return false.
	close(s.resultCh)
	for range s.resultCh {
	}
}

type topicQueryResult struct {
	src *enode.Node

	topicNodes []*enode.Node
	auxNodes   []*enode.Node
	err        error
}

func (s *topicSearch) runRequests(sys *topicSystem) {
	defer s.wg.Done()

	for job := range s.queryCh {
		result := sys.transport.topicQuery(job.dst, s.topic, job.buckets, s.opid)
		result.src = job.dst

		// Send response to main loop.
		select {
		case s.queryRespCh <- result:
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
