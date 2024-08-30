package discover

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

type fixedRoute []*enode.Node

func (r *fixedRoute) init(*Table) bool {
	return true
}

func (r *fixedRoute) nextHop() (*enode.Node, any) {
	if len(*r) == 0 {
		return nil, nil
	}
	n := (*r)[0]
	*r = (*r)[1:]
	return n, nil
}

func (r *fixedRoute) addFoundNodes(ns []*enode.Node) {}

func (r *fixedRoute) result() []*enode.Node {
	return nil
}

func TestWalk(t *testing.T) {
	transport := newPingRecorder()
	tab, db := newTestTable(transport, Config{})
	defer db.Close()
	defer tab.close()

	route := fixedRoute{
		enode.SignNull(new(enr.Record), enode.ID{1}),
		enode.SignNull(new(enr.Record), enode.ID{2}),
		enode.SignNull(new(enr.Record), enode.ID{3}),
		enode.SignNull(new(enr.Record), enode.ID{4}),
	}
	w := newWalk(tab, &route)
	q0, done := w.advance()
	if done {
		t.Fatal("expected done=false")
	}
	if q0 == nil {
		t.Fatal("expected query")
	}
	q1, done := w.advance()
	if done {
		t.Fatal("expected done=false")
	}
	q2, done := w.advance()
	if done {
		t.Fatal("expected done=false")
	}
	w.handleResponse(q2)
	w.handleResponse(q0)
	w.handleResponse(q1)

	q3, done := w.advance()
	if done {
		t.Fatal("expected done=false")
	}
	if q3 == nil {
		t.Fatal("expected query")
	}
	w.handleResponse(q3)

	q4, done := w.advance()
	if !done {
		t.Fatal("expected done=true")
	}
	if q4 != nil {
		t.Fatal("didn't expect query when done")
	}
}

// routeSim can simulate routing in a fake network.
type routeSim struct {
	table   *Table
	network map[enode.ID]*routeSimKad
	ids     []enode.ID
}

type routeSimConfig struct {
	NetworkSize int
	Seed        int64
}

// routeSimKad is a Kademlia table for use in routeSim.
type routeSimKad struct {
	self    *enode.Node
	buckets [nBuckets][]*enode.Node
}

func newRouteSim(cfg routeSimConfig) *routeSim {
	rs := new(routeSim)
	rng := rand.New(rand.NewSource(cfg.Seed))

	rs.network = make(map[enode.ID]*routeSimKad, cfg.NetworkSize)
	rs.ids = make([]enode.ID, cfg.NetworkSize)
	for i := 0; i < cfg.NetworkSize; i++ {
		node := randomNode(rng, i)
		rs.ids[i] = node.ID()
		rs.add(node)
	}
	slices.SortFunc(rs.ids, func(a, b enode.ID) int {
		return bytes.Compare(a[:], b[:])
	})

	// connect nodes
	for index, id := range rs.ids {
		// get neighbors left side
		for i := 1; i < 16; i++ {
			ni := (index - i) % len(rs.ids)
			rs.crossConnect(id, rs.ids[ni])
		}
		// get neighbors right side
		for i := 1; i < 16; i++ {
			ni := (index + i) % len(rs.ids)
			rs.crossConnect(id, rs.ids[ni])
		}
		// get some random neighbors
		for i := 0; i < 10; i++ {
			n2 := rs.ids[rng.Intn(len(rs.ids))]
			if n2 == id {
				i--
			} else {
				rs.crossConnect(id, n2)
			}
		}
	}
	return rs
}

// crossConnect registers two nodes with each other.
func (rs *routeSim) crossConnect(n1, n2 enode.ID) {
	rs.connect(n1, n2)
	rs.connect(n2, n1)
}

// connect registers n2 in n1's table.
func (rs *routeSim) connect(n1, n2 enode.ID) {
	if n1 == n2 {
		panic("connect of node to self")
	}
	tab1 := rs.network[n1]
	tab1.addNode(rs.network[n2].self)
}

// add creates a node in the simulated network.
func (rs *routeSim) add(n *enode.Node) {
	if _, ok := rs.network[n.ID()]; ok {
		panic("add of existing node")
	}
	kad := routeSimKad{self: n}
	rs.network[n.ID()] = &kad
}

// node returns the i'th node.
func (rs *routeSim) node(i int) *enode.Node {
	return rs.network[rs.ids[i]].self
}

func (rs *routeSim) nodeTable(i int) *Table {
	return rs.network[rs.ids[i]].toTable()
}

// runLookupQuery implements walkTransport.
func (rs *routeSim) runLookupQuery(ctx context.Context, q *query) {
	kad := rs.network[q.node.ID()]
	if kad == nil {
		q.err = errors.New("node not found")
		return
	}
	switch t := q.target.(type) {
	case []uint:
		q.resp = kad.getBucketNodes(t, bucketSize)
	case v4wire.Pubkey:
		q.resp = kad.closest(t.ID(), bucketSize)
	}
}

// closest returns the closest known nodes to the given ID.
func (kad *routeSimKad) closest(id enode.ID, n int) []*enode.Node {
	bydist := nodesByDistance{target: id}
	for _, b := range kad.buckets {
		for _, node := range b {
			bydist.push(node, n)
		}
	}
	return bydist.entries
}

func (kad *routeSimKad) getBucketNodes(bd []uint, n int) []*enode.Node {
	result := make([]*enode.Node, 0, n)
	for _, d := range bd {
		for _, node := range *kad.bucket(d) {
			if len(result) >= n {
				return result
			}
			// TODO filter by dist again here
			result = append(result, node)
		}
	}
	return result
}

func (kad *routeSimKad) bucket(d uint) *[]*enode.Node {
	if d <= uint(bucketMinDistance) {
		return &kad.buckets[0]
	}
	return &kad.buckets[d-uint(bucketMinDistance)-1]
}

func (kad *routeSimKad) addNode(node *enode.Node) bool {
	d := enode.LogDist(node.ID(), kad.self.ID())
	if d == 0 {
		return false
	}
	b := kad.bucket(uint(d))
	if len(*b) >= bucketSize {
		return false
	}
	match := func(e *enode.Node) bool { return e.ID() == node.ID() }
	if slices.ContainsFunc(*b, match) {
		return false
	}
	*b = append(*b, node)
	return true
}

func (kad *routeSimKad) toTable() *Table {
	db, _ := enode.OpenDB("")
	tr := newPingRecorder()
	tr.self = kad.self
	tab, _ := newTable(tr, db, Config{
		Clock: new(mclock.Simulated), // disable revalidation
	})
	go tab.loop()
	for _, b := range kad.buckets {
		for _, n := range b {
			ok := tab.addFoundNode(n, true)
			if !ok {
				panic(fmt.Sprintf("add failed for node %v", n))
			}
		}
	}
	return tab
}

func randomNode(rng *rand.Rand, i int) *enode.Node {
	var r enr.Record
	r.Set(enr.WithEntry("i", uint(i)))
	r.Set(enr.IPv4{127, 0, 0, 1})
	r.Set(enr.UDP(30303))
	var id enode.ID
	rng.Read(id[:])
	return enode.SignNull(&r, id)
}

// type idTree[T any] struct {
// 	root *idTreeBranch[T]
// }
//
// type idTreeBranch[T any] struct {
// 	left  *idTreeBranch[T]
// 	right *idTreeBranch[T]
// 	leaf  T
// }
//
// func (tr *idTree[T]) insert(id enode.ID, value T) {
// 	tn := &tr.root
// 	for _, byte := range id {
// 		for i := 7; i >= 0; i-- {
// 			bit := (byte >> i) & 0x01
// 			if *tn == nil {
// 				*tn = new(idTreeBranch[T])
// 			}
// 			if bit == 0 {
// 				tn = &(*tn).left
// 			} else {
// 				tn = &(*tn).right
// 			}
// 		}
// 	}
// 	(*tn).leaf = value
// }
//
// func (tr *idTree[T]) lookup(id enode.ID) (T, bool) {
// 	tn := tr.root
// 	for _, byte := range id {
// 		for i := 7; i >= 0; i-- {
// 			if tn == nil {
// 				break
// 			}
// 			if bit := (byte >> i) & 0x01; bit == 0 {
// 				tn = tn.left
// 			} else {
// 				tn = tn.right
// 			}
// 		}
// 	}
// 	if tn == nil {
// 		var zero T
// 		return zero, false
// 	}
// 	return tn.leaf, true
// }
//
// func (tr *idTree) neighbors(id enode.ID) {
//
// }
