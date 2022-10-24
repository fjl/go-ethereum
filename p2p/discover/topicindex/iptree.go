package topicindex

import (
	"fmt"
	"math"
	"net"
)

type ipTree struct {
	root *ipTreeNode
	bits byte
}

type ipTreeNode struct {
	left    *ipTreeNode
	right   *ipTreeNode
	counter int
}

func newIPTree(bits byte) *ipTree {
	if bits != 32 && bits != 128 {
		panic(fmt.Errorf("invalid ipTree bits %d", bits))
	}
	return &ipTree{new(ipTreeNode), bits}
}

func (it *ipTree) normIP(ip net.IP) net.IP {
	switch it.bits {
	case 32:
		ipv4 := ip.To4()
		if ipv4 == nil {
			panic("ipTree(bits=32) operation on invalid address")
		}
		return ipv4
	case 128:
		ipv6 := ip.To16()
		if ipv6 == nil {
			panic("ipTree(bits=128) operation on invalid address")
		}
		return ipv6
	default:
		panic(fmt.Errorf("invalid ipTree bits %d", it.bits))
	}
}

// insert adds an IP address to the tree and returns the similarity score.
func (it *ipTree) insert(ip net.IP) float64 {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	rootCounter := float64(it.root.counter)
	it.root.counter++

	for depth := byte(0); depth < it.bits; depth++ {
		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			*node = new(ipTreeNode)
		}
		n := *node

		balanced := rootCounter / math.Pow(2, float64(depth+1))
		if float64(n.counter) > balanced {
			sum++
		}
		n.counter++
	}
	return it.computeScore(sum)
}

// score computes the score that the addition of an IP would return.
func (it *ipTree) score(ip net.IP) float64 {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	rootCounter := float64(it.root.counter)

	for depth := byte(0); depth < it.bits; depth++ {
		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			break
		}
		n := *node

		balanced := rootCounter / math.Pow(2, float64(depth+1))
		if float64(n.counter) > balanced {
			sum++
		}
	}
	return it.computeScore(sum)
}

// remove removes an IP from the tree.
func (it *ipTree) remove(ip net.IP) {
	ip = it.normIP(ip)
	node := &it.root
	it.root.counter--

	for depth := byte(0); depth < it.bits; depth++ {
		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			return
		}
		n := *node
		n.counter--

		// If this was the last IP in this node, remove the branch.
		if n.counter == 0 {
			*node = nil
			return
		}

	}
}

// count returns the total number of IP addresses in tree.
func (it *ipTree) count() int {
	if it.root == nil {
		return 0
	}
	return it.root.counter
}

func (it *ipTree) computeScore(sum int) float64 {
	c := it.count()
	if c == 0 {
		return 0
	}
	sc := float64(sum) / float64(int(it.bits))
	return sc
}

func ipBit(ip net.IP, i byte) bool {
	return ip[i/8]&(1<<(7-i%8)) != 0
}
