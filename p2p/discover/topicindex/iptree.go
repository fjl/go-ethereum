package topicindex

import (
	"fmt"
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
	return &ipTree{nil, bits}
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
	for depth := byte(0); depth < it.bits; depth++ {
		if *node == nil {
			*node = new(ipTreeNode)
		}
		n := *node
		sum += n.counter
		n.counter++

		if ipBit(ip, depth) {
			node = &n.left
		} else {
			node = &n.right
		}
	}
	return it.computeScore(sum, 0)
}

// score computes the score that the addition of an IP would return.
func (it *ipTree) score(ip net.IP) float64 {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	for depth := byte(0); depth < it.bits; depth++ {
		if *node == nil {
			break
		}
		n := *node
		sum += n.counter

		if ipBit(ip, depth) {
			node = &n.left
		} else {
			node = &n.right
		}
	}
	return it.computeScore(sum, 1)
}

// remove removes an IP from the tree.
func (it *ipTree) remove(ip net.IP) {
	ip = it.normIP(ip)
	node := &it.root
	for depth := byte(0); depth < it.bits; depth++ {
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

		if ipBit(ip, depth) {
			node = &n.left
		} else {
			node = &n.right
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

func (it *ipTree) computeScore(sum int, additional int) float64 {
	c := it.count() + additional
	if c == 0 {
		return 0
	}
	sc := float64(sum) / float64(c*int(it.bits))
	// fmt.Printf("score sum:%d count:%d sc:%f\n", sum, c, sc)
	return sc
}

func ipBit(ip net.IP, i byte) bool {
	return ip[i/8]&(1<<(7-i%8)) != 0
}
