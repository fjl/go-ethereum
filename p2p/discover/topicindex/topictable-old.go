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

//go:build none
// +build none

package topicindex

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const MinWaitTime = 1 * time.Minute

// Config is the configuration of the topic system.
type Config struct {
	RegLifetime time.Duration
	TableLimit  int

	// These settings are for testing.
	Clock mclock.Clock
	Log   log.Logger
}

func (cfg Config) withDefaults() Config {
	if cfg.RegLifetime == 0 {
		cfg.RegLifetime = 15 * time.Minute
	}
	if cfg.TableLimit == 0 {
		cfg.TableLimit = 5000
	}

	if cfg.Log == nil {
		cfg.Log = log.Root()
	}
	if cfg.Clock == nil {
		cfg.Clock = mclock.System{}
	}
	return cfg
}

// TopicTable holds node registrations.
type TopicTable struct {
	reg      map[TopicID][]topicReg
	regCount int

	farTopic TopicID
	hostID   enode.ID

	config Config
}

type topicReg struct {
	node *enode.Node
	exp  mclock.AbsTime
}

// NewTopicTable creates a TopicTable.
func NewTopicTable(hostID enode.ID, cfg Config) *TopicTable {
	return &TopicTable{
		reg:      make(map[TopicID][]topicReg),
		farTopic: TopicID(hostID),
		hostID:   hostID,
		config:   cfg.withDefaults(),
	}
}

// Nodes returns all nodes registered for a topic.
func (tab *TopicTable) Nodes(topic TopicID) []*enode.Node {
	now := tab.config.Clock.Now()
	reglist := tab.reg[topic]
	nodes := make([]*enode.Node, 0, len(reglist))
	for _, reg := range reglist {
		if reg.exp > now {
			nodes = append(nodes, reg.node)
		}
	}
	return nodes
}

// Add adds a registration of node n for a topic.
func (tab *TopicTable) Add(n *enode.Node, topic TopicID) bool {
	for _, r := range tab.reg[topic] {
		if r.node.ID() == n.ID() {
			return true // n is already registered.
		}
	}

	// If there is space available, just add the registration.
	if tab.regCount < tab.config.TableLimit {
		tab.add(n, topic)
		return true
	}

	// There is no space. Check if this topic is closer to hostID than
	// the farthest existing one.
	if enode.DistCmp(tab.hostID, enode.ID(topic), enode.ID(tab.farTopic)) < 0 {
		// It's closer, so replace the oldest registration in that topic.
		tab.remove(tab.farTopic, 0)
		tab.add(n, topic)
		return true
	}

	return false
}

func (tab *TopicTable) add(n *enode.Node, topic TopicID) {
	tab.reg[topic] = append(tab.reg[topic], topicReg{
		node: n,
		exp:  tab.config.Clock.Now().Add(tab.config.RegLifetime),
	})
	tab.regCount++

	// Update farTopic.
	if enode.DistCmp(tab.hostID, enode.ID(tab.farTopic), enode.ID(topic)) < 0 {
		tab.farTopic = topic
	}
}

func (tab *TopicTable) remove(topic TopicID, index int) {
	list := tab.reg[topic]
	if len(list) <= index {
		panic(fmt.Sprintf("remove index %d out-of-bounds for tab.reg[%x] (len %d)", index, topic[:], len(list)))
	}

	tab.regCount--

	if len(list) == 1 {
		// It only has one entry left, remove the entire list.
		delete(tab.reg, topic)
		if topic == tab.farTopic {
			tab.farTopic = tab.findMostDistantTopic()
		}
	} else {
		tab.reg[topic] = append(list[:index], list[index+1:]...)
	}
}

// Expire removes inactive registrations.
func (tab *TopicTable) Expire() {
	now := tab.config.Clock.Now()
	topicRemoved := false

	for topic, regs := range tab.reg {
		active := activeRegIndex(now, regs)
		if active == len(regs) {
			// All expired, just delete the whole list.
			delete(tab.reg, topic)
			topicRemoved = true
		} else if active > 0 {
			// Remove expired prefix.
			tab.reg[topic] = append(regs[:0], regs[active:]...)
		}
	}

	if topicRemoved {
		tab.farTopic = tab.findMostDistantTopic()
	}
}

func activeRegIndex(now mclock.AbsTime, regs []topicReg) int {
	for i, reg := range regs {
		if reg.exp > now {
			return i
		}
	}
	return len(regs)
}

func (tab *TopicTable) findMostDistantTopic() TopicID {
	result := TopicID(tab.hostID)
	for topic := range tab.reg {
		if enode.DistCmp(tab.hostID, enode.ID(result), enode.ID(topic)) < 0 {
			result = topic
		}
	}
	return result
}
