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

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type topicRegController struct {
	transport *UDPv5
	config    topicindex.Config

	mu  sync.Mutex
	reg map[topicindex.TopicID]*topicReg
}

func newTopicRegController(transport *UDPv5, config topicindex.Config) *topicRegController {
	return &topicRegController{
		transport: transport,
		config:    config,
		reg:       make(map[topicindex.TopicID]*topicReg),
	}
}

func (rc *topicRegController) startTopic(topic topicindex.TopicID) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, ok := rc.reg[topic]; ok {
		return
	}
	rc.reg[topic] = newTopicReg(rc, topic)
}

func (rc *topicRegController) stopTopic(topic topicindex.TopicID) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if reg := rc.reg[topic]; reg != nil {
		reg.stop()
		delete(rc.reg, topic)
	}
}

func (rc *topicRegController) stopAllTopics() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for topic, reg := range rc.reg {
		reg.stop()
		delete(rc.reg, topic)
	}
}

// topicReg handles registering for a single topic.
type topicReg struct {
	state *topicindex.Registration
	wg    sync.WaitGroup
	quit  chan struct{}

	lookupCtx     context.Context
	lookupCancel  context.CancelFunc
	lookupTarget  chan enode.ID
	lookupResults chan []*enode.Node

	regRequest  chan *topicindex.RegAttempt
	regResponse chan *v5wire.Regconfirmation
}

func newTopicReg(rc *topicRegController, topic topicindex.TopicID) *topicReg {
	ctx, cancel := context.WithCancel(context.Background())
	reg := &topicReg{
		state:         topicindex.NewRegistration(topic, rc.config),
		quit:          make(chan struct{}),
		lookupCtx:     ctx,
		lookupCancel:  cancel,
		lookupTarget:  make(chan enode.ID),
		lookupResults: make(chan []*enode.Node),
		regRequest:    make(chan *topicindex.RegAttempt),
		regResponse:   make(chan *v5wire.Regconfirmation),
	}
	reg.wg.Add(3)
	go reg.run(rc)
	go reg.runRequests(rc)
	go reg.runLookups(rc)
	return reg
}

func (reg *topicReg) stop() {
	close(reg.quit)
	reg.wg.Wait()
}

func (reg *topicReg) run(rc *topicRegController) {
	defer reg.wg.Done()

	var lastAttempt *topicindex.RegAttempt

	for {
		// Enable request relay if the registration state wants to send one.
		var reqCh chan<- *topicindex.RegAttempt
		attemptToSend := reg.state.NextRequest()
		if attemptToSend != nil {
			reqCh = reg.regRequest
		}

		select {
		// Lookup management.
		case reg.lookupTarget <- reg.state.LookupTarget():
		case nodes := <-reg.lookupResults:
			reg.state.AddNodes(nodes)

		// Registration requests.
		case reqCh <- attemptToSend:
			lastAttempt = attemptToSend
		case resp := <-reg.regResponse:
			n := lastAttempt.Node
			if len(resp.Ticket) > 0 {
				// TODO: handle overflow
				wt := time.Duration(resp.WaitTime) * time.Second
				reg.state.HandleTicketResponse(n, resp.Ticket, wt)
			} else {
				rc.transport.log.Trace("Topic registration successful", "topic", reg.state.Topic(), "id", n.ID())
				reg.state.HandleRegistered(n)
			}
			lastAttempt = nil

		case <-reg.quit:
			// Quit runRequests, runLookups.
			close(reg.regRequest)
			close(reg.lookupTarget)
			reg.lookupCancel()
			return
		}
	}
}

func (reg *topicReg) runLookups(rc *topicRegController) {
	defer reg.wg.Done()

	for target := range reg.lookupTarget {
		l := rc.transport.newLookup(reg.lookupCtx, target)
		for l.advance() {
			// Send results of this step over to the main loop.
			nodes := unwrapNodes(l.replyBuffer)
			select {
			case reg.lookupResults <- nodes:
			case <-reg.lookupCtx.Done():
				return
			}
		}
	}
}

// runRequests performs topic registration requests.
// TODO: this is not great because it sends one at a time and waits for a response.
// registrations could just be sent async.
func (reg *topicReg) runRequests(rc *topicRegController) {
	defer reg.wg.Done()

	for attempt := range reg.regRequest {
		n := attempt.Node
		topic := reg.state.Topic()
		resp, err := rc.transport.topicRegister(n, topic, attempt.Ticket)
		if err != nil {
			rc.transport.log.Debug("Topic registration failed", "topic", topic, "id", n.ID(), "err", err)
			continue
		}

		// Send response to main loop.
		select {
		case reg.regResponse <- resp:
		case <-reg.quit:
			return
		}
	}
}
