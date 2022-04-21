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
	"container/list"
	"math"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// wait time computation constants.
const (
	ipModifierExp    = 0.4
	idModifierExp    = 0.4
	topicModifierExp = 15
	occupancyExp     = 4
	baseMultiplier   = 30
)

// TopicTable holds node registrations.
type TopicTable struct {
	all *list.List
	reg map[TopicID]*list.List
	wt  waitTimeState

	hostID enode.ID
	config Config
}

type topicTableEntry struct {
	exp   mclock.AbsTime
	node  *enode.Node
	topic TopicID

	topicElem *list.Element
	allElem   *list.Element
}

// NewTopicTable creates a TopicTable.
func NewTopicTable(hostID enode.ID, cfg Config) *TopicTable {
	return &TopicTable{
		reg:    make(map[TopicID]*list.List),
		all:    list.New(),
		wt:     newWaitTimeState(),
		hostID: hostID,
		config: cfg.withDefaults(),
	}
}

// Nodes returns all nodes registered for a topic.
func (tab *TopicTable) Nodes(topic TopicID) []*enode.Node {
	now := tab.config.Clock.Now()
	reglist := tab.reg[topic]
	if reglist == nil {
		return []*enode.Node{}
	}
	nodes := make([]*enode.Node, 0, reglist.Len())
	for e := reglist.Front(); e != nil; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		if reg.exp > now {
			nodes = append(nodes, reg.node)
		}
	}
	return nodes
}

// NextExpiryTime returns the time when the next registration expires.
func (tab *TopicTable) NextExpiryTime() mclock.AbsTime {
	e := tab.all.Front()
	if e != nil {
		return e.Value.(*topicTableEntry).exp
	}
	return Never
}

// Expire removes inactive registrations.
func (tab *TopicTable) Expire() {
	now := tab.config.Clock.Now()
	for e := tab.all.Front(); e != nil; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		if reg.exp > now {
			break
		}
		tab.remove(reg)
		tab.wt.removeReg(reg)
	}
}

// nextTopicExpiryTime returns the time when the next registration of topic t expires.
func (tab *TopicTable) nextTopicExpiryTime(t TopicID) mclock.AbsTime {
	list := tab.reg[t]
	if list == nil {
		return 0
	}
	e := list.Front()
	if e == nil {
		return 0 // cannot happen
	}
	return e.Value.(*topicTableEntry).exp
}

// isRegistered reports whether n is currently registered for topic t.
func (tab *TopicTable) isRegistered(n *enode.Node, t TopicID) bool {
	list := tab.reg[t]
	if list != nil {
		for el := list.Front(); el != nil; el = el.Next() {
			if el.Value.(*topicTableEntry).node.ID() == n.ID() {
				return true
			}
		}
	}
	return false
}

// Add adds a registration of node n for a topic. This only works when the table
// has space available.
func (tab *TopicTable) Add(n *enode.Node, topic TopicID) bool {
	if tab.all.Len() < tab.config.TableLimit {
		tab.add(n, topic)
		return true
	}
	return false
}

func (tab *TopicTable) add(n *enode.Node, topic TopicID) *topicTableEntry {
	reg := &topicTableEntry{
		node:  n,
		exp:   tab.config.Clock.Now().Add(tab.config.RegLifetime),
		topic: topic,
	}
	if tab.reg[topic] == nil {
		tab.reg[topic] = list.New()
	}
	reg.topicElem = tab.reg[topic].PushFront(reg)
	reg.allElem = tab.all.PushFront(reg)
	tab.wt.addReg(reg)
	return reg
}

func (tab *TopicTable) remove(reg *topicTableEntry) {
	tab.all.Remove(reg.allElem)
	topicList := tab.reg[reg.topic]
	if topicList.Len() == 1 {
		delete(tab.reg, reg.topic)
	} else {
		topicList.Remove(reg.topicElem)
	}
	reg.topicElem = nil
	reg.allElem = nil
}

// topicSize returns the number of nodes registered for topic t.
func (tab *TopicTable) topicSize(t TopicID) int {
	list := tab.reg[t]
	if list == nil {
		return 0
	}
	return list.Len()
}

// WaitTime returns the amount of time that node n must have waited to register for topic t.
func (tab *TopicTable) WaitTime(n *enode.Node, t TopicID) time.Duration {
	regCount := tab.all.Len()

	occupancy := 1.0 - (float64(regCount) / float64(tab.config.TableLimit))
	occupancyScore := float64(tab.config.RegLifetime/time.Second) / math.Pow(occupancy, occupancyExp)

	topicMod := math.Pow(float64(tab.topicSize(t))/float64(regCount+1), topicModifierExp)
	ipMod := tab.wt.ipModifier(n)
	idMod := tab.wt.idModifier(n, regCount)

	neededTime := baseMultiplier * occupancyScore * math.Max(topicMod+ipMod+idMod, 0.000001)
	return time.Duration(math.Ceil(neededTime * float64(time.Second)))
}

// Register adds node n for topic t if it has waited long enough.
func (tab *TopicTable) Register(n *enode.Node, t TopicID, waitTime time.Duration) time.Duration {
	// Reject attempt if node is already registered.
	if tab.isRegistered(n, t) {
		return 0
	}

	// Check if the node has waited enough.
	requiredTime := tab.WaitTime(n, t)
	if waitTime < requiredTime {
		return requiredTime - waitTime
	}

	// Check if there is space. If not, the node needs to come back when a slot opens.
	if tab.all.Len() >= tab.config.TableLimit {
		now := tab.config.Clock.Now()
		return tab.NextExpiryTime().Sub(now)
	}

	tab.add(n, t)
	return 0
}

// Note about lower bound removal: Lower bound information only needs to be kept for
// active registration topic/id/ip, because only active registrations influence the
// waiting time modifier value. The lower-bound value kept is a tuple of (value,
// timestamp). After time wt has expired (at timestamp+wt), the tuple can be deleted.

// waitTimeState holds the state of waiting time modifier functions.
type waitTimeState struct {
	idCounter map[enode.ID]int
	ipv4      *ipTree
	ipv6      *ipTree
}

func newWaitTimeState() waitTimeState {
	return waitTimeState{
		idCounter: make(map[enode.ID]int),
		ipv4:      newIPTree(32),
		ipv6:      newIPTree(128),
	}
}

func (wt *waitTimeState) ipModifier(n *enode.Node) float64 {
	var (
		ip4    enr.IPv4
		ip6    enr.IPv6
		score4 float64
		score6 float64
	)
	if n.Load(&ip4) == nil {
		score4 = wt.ipv4.score(net.IP(ip4))
	}
	if n.Load(&ip6) == nil {
		score6 = wt.ipv6.score(net.IP(ip6))
	}
	return math.Max(score4, score6)
}

func (wt *waitTimeState) idModifier(n *enode.Node, regCount int) float64 {
	counter := float64(wt.idCounter[n.ID()])
	return math.Pow(counter/float64(regCount+1), idModifierExp)
}

func (wt *waitTimeState) addReg(reg *topicTableEntry) {
	wt.idCounter[reg.node.ID()]++

	// Add IPs.
	var ip4 enr.IPv4
	var ip6 enr.IPv6
	if reg.node.Load(&ip4) == nil {
		wt.ipv4.insert(net.IP(ip4))
	}
	if reg.node.Load(&ip6) == nil {
		wt.ipv6.insert(net.IP(ip6))
	}
}

func (wt *waitTimeState) removeReg(reg *topicTableEntry) {
	// Remove from idCounter.
	id := reg.node.ID()
	idc := wt.idCounter[id]
	if idc == 1 {
		delete(wt.idCounter, id)
	} else {
		wt.idCounter[id] = idc - 1
	}

	// Remove IPs.
	var ip4 enr.IPv4
	var ip6 enr.IPv6
	if reg.node.Load(&ip4) == nil {
		wt.ipv4.remove(net.IP(ip4))
	}
	if reg.node.Load(&ip6) == nil {
		wt.ipv6.remove(net.IP(ip6))
	}
}
