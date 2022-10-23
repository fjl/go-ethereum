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
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const totalNodesResponseLimit = 16

var (
	errNodeNetRestrict = errors.New("not contained in netrestrict list")
	errNodeDuplicate   = errors.New("duplicate record")
)

// nodesProc accumulates response messages containing ENRs.
type nodesProc struct {
	cfg          *Config
	total        int
	received     int
	resultBuffer []*enode.Node
	seen         map[enode.ID]struct{}
	limit        int // max. number of nodes
	reqDistances []uint
}

func newNodesProc(cfg *Config, limit int, requestDistances []uint) *nodesProc {
	np := &nodesProc{cfg: cfg}
	return np.reset(limit, requestDistances)
}

// reset re-initializes the processor for a new request.
func (np *nodesProc) reset(limit int, requestDistances []uint) *nodesProc {
	// Reuse 'seen' map.
	seen := np.seen
	if seen == nil {
		seen = make(map[enode.ID]struct{}, limit)
	} else {
		for k := range seen {
			delete(seen, k)
		}
	}
	*np = nodesProc{
		cfg:          np.cfg,
		total:        -1,
		resultBuffer: np.resultBuffer[:0],
		seen:         seen,
		limit:        limit,
		reqDistances: requestDistances,
	}
	return np
}

// result returns the result nodes. The return value is a copy of the internal
// buffer, and can be shared with other goroutines.
func (np *nodesProc) result() []*enode.Node {
	r := make([]*enode.Node, len(np.resultBuffer))
	copy(r, np.resultBuffer)
	return r
}

// done reports whether all response messages have arrived.
func (np *nodesProc) done() bool {
	if np.total == -1 {
		return false // no response received yet
	}
	return np.received >= np.total || len(np.resultBuffer) >= np.limit
}

// setTotal sets the total number of response messages.
func (np *nodesProc) setTotal(count uint8) {
	if np.total != -1 {
		return // already set
	}
	if count > totalNodesResponseLimit {
		count = totalNodesResponseLimit
	}
	np.total = int(count)
}

// addResponse adds to the response counter.
func (np *nodesProc) addResponse() {
	np.received++
}

// addNodes processes nodes from the response.
func (np *nodesProc) addNodes(src *enode.Node, records []*enr.Record, reqType string) {
	for _, record := range records {
		if len(np.resultBuffer) >= np.limit {
			break
		}
		node, err := np.verify(src, record)
		if err != nil {
			np.cfg.Log.Debug(reqType+" contains invalid record", "id", src.ID(), "err", err)
			continue
		}
		np.resultBuffer = append(np.resultBuffer, node)
	}
}

// verify checks the validity of a response node.
func (np *nodesProc) verify(src *enode.Node, r *enr.Record) (*enode.Node, error) {
	node, err := enode.New(np.cfg.ValidSchemes, r)
	if err != nil {
		return nil, err
	}
	if _, ok := np.seen[node.ID()]; ok {
		return nil, errNodeDuplicate
	}
	if err := netutil.CheckRelayIP(src.IP(), node.IP()); err != nil {
		return nil, err
	}
	if node.UDP() <= 1024 {
		return nil, errLowPort
	}
	if np.cfg.NetRestrict != nil && !np.cfg.NetRestrict.Contains(node.IP()) {
		return nil, errNodeNetRestrict
	}
	if np.reqDistances != nil {
		nd := enode.LogDist(src.ID(), node.ID())
		if !containsUint(uint(nd), np.reqDistances) {
			return nil, fmt.Errorf("distance %d not contained in request %v", nd, np.reqDistances)
		}
	}
	return node, nil
}

func packFindnodeResponse(reqID []byte, nodes []*enode.Node) []*v5wire.Nodes {
	if len(nodes) == 0 {
		return []*v5wire.Nodes{{ReqID: reqID, ResponseCount: 1}}
	}
	packs := packNodes(nodes)
	var resps []*v5wire.Nodes
	for _, recs := range packs {
		resps = append(resps, &v5wire.Nodes{
			ReqID:         reqID,
			ResponseCount: uint8(len(packs)),
			Nodes:         recs,
		})
	}
	return resps
}

// packNodes chunks up nodes for the given node list.
func packNodes(nodes []*enode.Node) [][]*enr.Record {
	var resp [][]*enr.Record
	for len(nodes) > 0 {
		resp = append(resp, make([]*enr.Record, 0, 3))
		end := len(resp) - 1
		items := min(nodesResponseItemLimit, len(nodes))
		for i := 0; i < items; i++ {
			resp[end] = append(resp[end], nodes[i].Record())
		}
		nodes = nodes[items:]
	}
	return resp
}

func containsUint(x uint, xs []uint) bool {
	for _, v := range xs {
		if x == v {
			return true
		}
	}
	return false
}
