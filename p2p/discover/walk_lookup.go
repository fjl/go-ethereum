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
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type lookupRoute struct {
	target any
	list   nodesByDistance
	asked  map[enode.ID]bool
}

func newLookupRouteV4(target v4wire.Pubkey) *lookupRoute {
	return &lookupRoute{target: target, list: nodesByDistance{target: target.ID()}}
}

func newLookupRouteV5(target enode.ID) *lookupRoute {
	return &lookupRoute{target: target, list: nodesByDistance{target: target}}
}

func (l *lookupRoute) init(tab *Table) bool {
	closest := tab.findnodeByID(l.list.target, bucketSize, false)
	if len(closest.entries) == 0 {
		return false
	}
	l.asked = make(map[enode.ID]bool)
	l.list.entries = closest.entries
	return true
}

func (l *lookupRoute) nextHop() (*enode.Node, any) {
	for _, n := range l.list.entries {
		if !l.asked[n.ID()] {
			l.asked[n.ID()] = true
			return n, l.target
		}
	}
	return nil, l.target
}

func (l *lookupRoute) addFoundNodes(ns []*enode.Node) {
	for _, n := range ns {
		l.list.push(n, bucketSize)
	}
}

func (l *lookupRoute) result() []*enode.Node {
	return l.list.entries
}

// lookupDistances computes the distance parameter for FINDNODE calls to dest.
// It chooses distances adjacent to logdist(target, dest), e.g. for a target
// with logdist(target, dest) = 255 the result is [255, 256, 254].
func lookupDistances(target, dest enode.ID) (dists []uint) {
	td := enode.LogDist(target, dest)
	dists = append(dists, uint(td))
	for i := 1; len(dists) < lookupRequestLimit; i++ {
		if td+i <= 256 {
			dists = append(dists, uint(td+i))
		}
		if td-i > 0 {
			dists = append(dists, uint(td-i))
		}
	}
	return dists
}
