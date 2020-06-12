package test

import (
	"crypto/ecdsa"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
)

type testenv struct {
	l1, l2     net.PacketConn
	key        *ecdsa.PrivateKey
	remote     string
	remoteAddr *net.UDPAddr
}

func newTestEnv(remote string) *testenv {
	l1, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	l2, err := net.ListenPacket("udp", "127.0.0.2:0")
	if err != nil {
		panic(err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}
	addr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		panic(err)
	}
	return &testenv{l1, l2, key, remote, addr}
}

func (te *testenv) close() {
	te.l1.Close()
	te.l2.Close()
}

func (te *testenv) send(c net.PacketConn, req v4wire.Packet) error {
	packet, _, err := v4wire.Encode(te.key, req)
	if err != nil {
		return err
	}
	_, err = c.WriteTo(packet, te.remoteAddr)
	return err
}

func (te *testenv) read(c net.PacketConn) (v4wire.Packet, []byte, error) {
	buf := make([]byte, 2048)
	if err := c.SetReadDeadline(time.Now().Add(time.Duration(*waitTime) * time.Millisecond)); err != nil {
		return nil, nil, err
	}
	n, _, err := c.ReadFrom(buf)
	if err != nil {
		return nil, nil, err
	}
	p, _, hash, err := v4wire.Decode(buf[:n])
	return p, hash, err
}

func (te *testenv) localEndpoint(c net.PacketConn) v4wire.Endpoint {
	addr := c.LocalAddr().(*net.UDPAddr)
	return v4wire.Endpoint{
		IP:  net.IP{127, 0, 0, 1},
		UDP: uint16(addr.Port),
		TCP: 0,
	}
}

func (te *testenv) remoteEndpoint() v4wire.Endpoint {
	return v4wire.NewEndpoint(te.remoteAddr, 0)
}

func contains(ns []v4wire.Node, key v4wire.Pubkey) bool {
	for _, n := range ns {
		if n.ID == key {
			return true
		}
	}
	return false
}
