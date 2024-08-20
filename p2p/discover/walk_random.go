package discover

import (
	"slices"

	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const randomRouteBuffer = 32

type randomRoute struct {
	buf []*enode.Node
	rng randomSource
}

func (r *randomRoute) init(tab *Table) bool {
	r.rng = &tab.rand
	if r.buf == nil {
		r.buf = make([]*enode.Node, randomRouteBuffer)
	}
	n := tab.readRandomNodes(r.buf[:randomRouteBuffer])
	r.buf = r.buf[:n]
	return n > 0
}

// nextNode selects the next hop and removes it from the buffer.
func (r *randomRoute) nextNode() *enode.Node {
	if len(r.buf) == 0 {
		return nil
	}
	index := r.rng.Intn(len(r.buf))
	n := r.buf[index]
	r.buf = slices.Delete(r.buf, index, index+1)
	return n
}

// addFoundNodes adds two random nodes from the response to the route buffer. We add two
// because we want the buffer to grow if it isn't full, but also want to limit the
// contribution of any particular hop to the walk.
func (r *randomRoute) addFoundNodes(ns []*enode.Node) {
	switch len(ns) {
	case 0:
	case 1:
		r.add(ns[0])
	default:
		i1 := r.rng.Intn(len(ns))
		i2 := i1
		for i2 == i1 {
			i2 = r.rng.Intn(len(ns))
		}
		r.add(ns[i1])
		r.add(ns[i2])
	}
}

func (r *randomRoute) add(n *enode.Node) {
	if len(r.buf) < randomRouteBuffer {
		r.buf = append(r.buf, n)
	} else {
		r.buf[r.rng.Intn(len(r.buf))] = n
	}
}

func (r *randomRoute) result() []*enode.Node {
	return nil
}

// randomRouteV4 is a random walk for discv4.
type randomRouteV4 struct {
	randomRoute
}

func newRandomRouteV4(rng randomSource) *randomRouteV4 {
	return &randomRouteV4{randomRoute{rng: rng}}
}

func (r *randomRouteV4) nextHop() (*enode.Node, any) {
	var target v4wire.Pubkey
	n := r.nextNode()
	if n != nil {
		r.rng.Read(target[:])
	}
	return n, target
}

// randomRouteV5 is a random walk for discv5.
type randomRouteV5 struct {
	randomRoute
}

func newRandomRouteV5(rng randomSource) *randomRouteV5 {
	return &randomRouteV5{randomRoute{rng: rng}}
}

func (r *randomRouteV5) nextHop() (*enode.Node, any) {
	n := r.nextNode()
	targets := []uint{256, 255, 254, 253} // furthest buckets
	return n, targets
}
