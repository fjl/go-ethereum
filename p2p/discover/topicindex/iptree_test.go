package topicindex

import (
	"crypto/rand"
	"math"
	"net"
	"testing"
)

type treeAddOp struct {
	ip    string
	score float64
}

var ipv4TestOps = []treeAddOp{
	{"1.2.3.4", 0.0},
	{"1.2.3.5", 0.96875},
	{"1.2.3.6", 0.9375},
	{"1.2.3.4", 1},
	{"1.2.3.4", 1},
	{"1.2.3.4", 1},
	{"1.1.1.1", 0.4375},
	{"8.8.8.8", 0.125},
}

var ipv6TestOps = []treeAddOp{
	{"fe80::716b:dafc:774e:a826", 0.0},
	{"fe80::716b:dafc:774e:a827", 0.9921875},
	{"fe80::716b:dafc:774e:a828", 0.96875},
	{"abc2:2345:7a7b:774e:a828:f184:ee25:4545", 0.0078125},
}

func TestIPTreeInsert(t *testing.T) {
	t.Run("IPv4", func(t *testing.T) {
		it := newIPTree(32)
		for i, add := range ipv4TestOps {
			sc := it.insert(net.ParseIP(add.ip))
			if !almostEQ(sc, add.score) {
				t.Errorf("score %g not equal to expected score %g when adding IP %s", sc, add.score, add.ip)
			}
			if c := it.count(); c != i+1 {
				t.Errorf("wrong count %d at op %d", c, i)
			}
		}
	})
	t.Run("IPv6", func(t *testing.T) {
		it := newIPTree(128)
		for i, add := range ipv6TestOps {
			sc := it.insert(net.ParseIP(add.ip))
			if !almostEQ(sc, add.score) {
				t.Errorf("score %g not equal to expected score %g when adding IP %s", sc, add.score, add.ip)
			}
			if c := it.count(); c != i+1 {
				t.Errorf("wrong count %d at op %d", c, i)
			}
		}
	})
}

func TestIPTreeScore(t *testing.T) {
	randomAdd := func(t *testing.T, it *ipTree) {
		ip := make(net.IP, it.bits/8)
		rand.Read(ip)
		sc1 := it.score(ip)
		sc2 := it.insert(ip)
		if sc1 != sc2 {
			t.Fatalf("score() result %f not equal insert() result %f, IP: %v", sc1, sc2, ip)
		}
	}

	t.Run("IPv4", func(t *testing.T) {
		it := newIPTree(32)
		for i := 0; i < 1000; i++ {
			randomAdd(t, it)
		}
	})
	t.Run("IPv6", func(t *testing.T) {
		it := newIPTree(128)
		for i := 0; i < 1000; i++ {
			randomAdd(t, it)
		}
	})
}

func TestIPTreeRemove(t *testing.T) {
	it := newIPTree(32)
	for _, op := range ipv4TestOps {
		it.insert(net.ParseIP(op.ip))
	}

	// Remove the IPs backwards, and check that score() matches after removing.
	for i := len(ipv4TestOps) - 1; i >= 0; i-- {
		op := ipv4TestOps[i]
		ip := net.ParseIP(op.ip)

		it.remove(ip)
		if sc := it.score(ip); !almostEQ(sc, op.score) {
			t.Errorf("score %g does not match expected score %g after removing %v", sc, op.score, ip)
		}
		if c := it.count(); c != i {
			t.Errorf("wrong count %d at op %d", c, i)
		}
	}
}

func almostEQ(a, b float64) bool {
	return math.Abs(a-b) < 1e-08
}
