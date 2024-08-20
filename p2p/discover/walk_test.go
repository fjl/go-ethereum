package discover

import (
	"testing"

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
