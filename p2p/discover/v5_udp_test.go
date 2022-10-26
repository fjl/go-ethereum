// Copyright 2020 The go-ethereum Authors
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
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

// Real sockets, real crypto: this test checks end-to-end connectivity for UDPv5.
func TestUDPv5_lookupE2E(t *testing.T) {
	t.Parallel()

	const N = 5
	var (
		nodes []*UDPv5
		cfg   = Config{PingInterval: 500 * time.Millisecond}
	)
	for i := 0; i < N; i++ {
		if i > 0 {
			bn := nodes[0].Self()
			cfg.Bootnodes = []*enode.Node{bn}
		}
		node := startLocalhostV5(t, cfg)
		nodes = append(nodes, node)
		defer node.Close()
	}
	last := nodes[N-1]
	target := nodes[rand.Intn(N-2)].Self()

	// Wait for network to initialize.
	time.Sleep((N + 1) * cfg.PingInterval)

	// It is expected that all nodes are found.
	// Note: the node running the query is excluded from results!
	var expectedResult []*enode.Node
	for _, node := range nodes[:N-1] {
		expectedResult = append(expectedResult, node.Self())
	}
	sort.Slice(expectedResult, func(i, j int) bool {
		return enode.DistCmp(target.ID(), expectedResult[i].ID(), expectedResult[j].ID()) < 0
	})

	// Do the lookup.
	results := last.Lookup(target.ID())
	if err := checkNodesEqual(results, expectedResult); err != nil {
		t.Fatalf("lookup returned wrong results: %v", err)
	}
}

func startLocalhostV5(t *testing.T, cfg Config) *UDPv5 {
	cfg.PrivateKey = newkey()
	db, _ := enode.OpenDB("")
	ln := enode.NewLocalNode(db, cfg.PrivateKey)

	// Prefix logs with node ID.
	lprefix := fmt.Sprintf("(%s)", ln.ID().TerminalString())
	lfmt := log.TerminalFormat(false)
	cfg.Log = testlog.Logger(t, log.LvlTrace)
	cfg.Log.SetHandler(log.FuncHandler(func(r *log.Record) error {
		t.Logf("%s %s", lprefix, lfmt.Format(r))
		return nil
	}))

	// Listen.
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IP{127, 0, 0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	realaddr := socket.LocalAddr().(*net.UDPAddr)
	ln.SetStaticIP(realaddr.IP)
	ln.SetFallbackUDP(realaddr.Port)
	udp, err := ListenV5(socket, ln, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return udp
}

// This test checks that incoming PING calls are handled correctly.
func TestUDPv5_pingHandling(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	test.packetIn(&v5wire.Ping{ReqID: []byte("foo")})
	test.requirePacketOut(func(p *v5wire.Pong, addr *net.UDPAddr, _ v5wire.Nonce) {
		if !bytes.Equal(p.ReqID, []byte("foo")) {
			t.Error("wrong request ID in response:", p.ReqID)
		}
		if p.ENRSeq != test.table.self().Seq() {
			t.Error("wrong ENR sequence number in response:", p.ENRSeq)
		}
	})
}

// This test checks that incoming 'unknown' packets trigger the handshake.
func TestUDPv5_unknownPacket(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	nonce := v5wire.Nonce{1, 2, 3}
	check := func(p *v5wire.Whoareyou, wantSeq uint64) {
		t.Helper()
		if p.Nonce != nonce {
			t.Error("wrong nonce in WHOAREYOU:", p.Nonce, nonce)
		}
		if p.IDNonce == ([16]byte{}) {
			t.Error("all zero ID nonce")
		}
		if p.RecordSeq != wantSeq {
			t.Errorf("wrong record seq %d in WHOAREYOU, want %d", p.RecordSeq, wantSeq)
		}
	}

	// Unknown packet from unknown node.
	test.packetIn(&v5wire.Unknown{Nonce: nonce})
	test.requirePacketOut(func(p *v5wire.Whoareyou, addr *net.UDPAddr, _ v5wire.Nonce) {
		check(p, 0)
	})

	// Make node known.
	n := test.getNode(test.remotekey, test.remoteaddr).Node()
	test.table.addSeenNode(wrapNode(n))

	test.packetIn(&v5wire.Unknown{Nonce: nonce})
	test.requirePacketOut(func(p *v5wire.Whoareyou, addr *net.UDPAddr, _ v5wire.Nonce) {
		check(p, n.Seq())
	})
}

// This test checks that incoming FINDNODE calls are handled correctly.
func TestUDPv5_findnodeHandling(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	// Create test nodes and insert them into the table.
	nodes253 := nodesAtDistance(test.table.self().ID(), 253, 10)
	nodes249 := nodesAtDistance(test.table.self().ID(), 249, 4)
	nodes248 := nodesAtDistance(test.table.self().ID(), 248, 10)
	fillTable(test.table, wrapNodes(nodes253))
	fillTable(test.table, wrapNodes(nodes249))
	fillTable(test.table, wrapNodes(nodes248))

	// Requesting with distance zero should return the node's own record.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{0}, Distances: []uint{0}})
	test.expectNodes(1, []byte{0}, []*enode.Node{test.udp.Self()})

	// Requesting with distance > 256 shouldn't crash.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{1}, Distances: []uint{4234098}})
	test.expectNodes(1, []byte{1}, nil)

	// Requesting with empty distance list shouldn't crash either.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{2}, Distances: []uint{}})
	test.expectNodes(1, []byte{2}, nil)

	// This request gets no nodes because the corresponding bucket is empty.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{3}, Distances: []uint{254}})
	test.expectNodes(1, []byte{3}, nil)

	// This request gets all the distance-253 nodes.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{4}, Distances: []uint{253}})
	test.expectNodes(4, []byte{4}, nodes253)

	// This request gets all the distance-249 nodes and some more at 248 because
	// the bucket at 249 is not full.
	test.packetIn(&v5wire.Findnode{ReqID: []byte{5}, Distances: []uint{249, 248}})
	var nodes []*enode.Node
	nodes = append(nodes, nodes249...)
	nodes = append(nodes, nodes248[:10]...)
	test.expectNodes(5, []byte{5}, nodes)
}

// This test checks that incoming TOPICQUERY calls are handled correctly.
func TestUDPv5_topicqueryHandling(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	// Create test nodes and insert them into the table.
	topic1 := topicindex.TopicID{1, 1, 1, 1}
	topic2 := topicindex.TopicID{9, 9, 9, 9}

	topicNodesList := nodesAtDistance(test.table.self().ID(), 256, 10)
	for _, n := range topicNodesList {
		test.udp.topicTable.Add(n, topic2)
	}

	t.Run("empty-resp", func(t *testing.T) {
		// There are no available nodes for topic1, so it gets an empty response.
		test.packetIn(&v5wire.TopicQuery{ReqID: []byte{0}, Topic: topic1})
		resp := test.expectMultipleResponses(1, []byte{0}, 1*time.Second)
		tnresp := packetsOfType[*v5wire.TopicNodes](resp)

		checkResponseCountField(t, resp, 1)
		checkResponseNodes(t, tnresp, nil)
	})

	t.Run("topicnodes-only", func(t *testing.T) {
		// Ask for nodes of topic2.
		test.packetIn(&v5wire.TopicQuery{ReqID: []byte{1}, Topic: topic2})
		resp := test.expectMultipleResponses(4, []byte{1}, 1*time.Second)
		tnresp := packetsOfType[*v5wire.TopicNodes](resp)

		// Check that TOPICNODES responses contain all the nodes.
		checkResponseCountField(t, resp, 4)
		checkResponseNodes(t, tnresp, topicNodesList)
	})

	// Now add some nodes to the main node table as well.
	neighborNodes := nodesAtDistance(enode.ID(topic1), 250, 6)
	fillTable(test.table, wrapNodes(neighborNodes))

	t.Run("nodes-only", func(t *testing.T) {
		// Ask for nodes of topic1. There are none, but the main table has
		// some nodes close to the topic, and they are returned.
		test.packetIn(&v5wire.TopicQuery{ReqID: []byte{3}, Topic: topic1})
		resp := test.expectMultipleResponses(2, []byte{3}, 1*time.Second)
		tnresp := packetsOfType[*v5wire.TopicNodes](resp)
		nresp := packetsOfType[*v5wire.Nodes](resp)

		// Check that TOPICNODES responses contain all the nodes.
		checkResponseCountField(t, resp, 2)
		checkResponseNodes(t, tnresp, nil)
		checkResponseNodes(t, nresp, neighborNodes)
	})

	t.Run("both-kinds", func(t *testing.T) {
		// Ask for nodes of topic1. There are none, but the main table has
		// some nodes close to the topic, and they are returned.
		test.packetIn(&v5wire.TopicQuery{ReqID: []byte{3}, Topic: topic2})
		resp := test.expectMultipleResponses(6, []byte{3}, 1*time.Second)

		tnresp := packetsOfType[*v5wire.TopicNodes](resp)
		nresp := packetsOfType[*v5wire.Nodes](resp)

		// Check that TOPICNODES responses contain all the nodes.
		checkResponseCountField(t, resp, 6)
		checkResponseNodes(t, tnresp, topicNodesList)
		checkResponseNodes(t, nresp, neighborNodes)
	})
}

// This test checks basic handling of REGTOPIC.
func TestUDPv5_regtopicHandling(t *testing.T) {
	t.Parallel()

	test := newUDPV5Test(t, Config{})
	defer test.close()

	topic1 := topicindex.TopicID{1, 1, 1, 1}
	neighborNodes := nodesAtDistance(enode.ID(topic1), 250, 6)
	fillTable(test.table, wrapNodes(neighborNodes))

	// Send the request.
	self := test.getNode(test.remotekey, test.remoteaddr)
	test.packetIn(&v5wire.Regtopic{
		ReqID: []byte{0},
		Topic: topic1,
		ENR:   self.Node().Record(),
	})

	// Send the response.
	resp := test.expectMultipleResponses(3, []byte{0}, 1*time.Second)
	confirmations := packetsOfType[*v5wire.Regconfirmation](resp)
	nresp := packetsOfType[*v5wire.Nodes](resp)

	checkResponseCountField(t, resp, 3)
	checkResponseNodes(t, nresp, neighborNodes)

	if len(confirmations) == 0 {
		t.Fatal("missing REGCONFIRMATION response")
	}
	cresp := confirmations[0]
	if len(cresp.Ticket) > 0 {
		t.Fatal("didn't register")
	}
	if len(test.udp.topicTable.Nodes(topic1)) != 1 {
		t.Fatal("didn't add to topic table")
	}
}

// This test checks that outgoing PING calls work.
func TestUDPv5_pingCall(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan error, 1)

	// This ping times out.
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {})
	if err := <-done; err != errTimeout {
		t.Fatalf("want errTimeout, got %q", err)
	}

	// This ping works.
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetInFrom(test.remotekey, test.remoteaddr, &v5wire.Pong{ReqID: p.ReqID})
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// This ping gets a reply from the wrong endpoint.
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		wrongAddr := &net.UDPAddr{IP: net.IP{33, 44, 55, 22}, Port: 10101}
		test.packetInFrom(test.remotekey, wrongAddr, &v5wire.Pong{ReqID: p.ReqID})
	})
	if err := <-done; err != errTimeout {
		t.Fatalf("want errTimeout for reply from wrong IP, got %q", err)
	}
}

// This test checks that outgoing FINDNODE calls work and multiple NODES
// replies are aggregated.
func TestUDPv5_findnodeCall(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	// Launch the request:
	var (
		distances = []uint{230}
		remote    = test.getNode(test.remotekey, test.remoteaddr).Node()
		nodes     = nodesAtDistance(remote.ID(), int(distances[0]), 8)
		done      = make(chan error, 1)
		response  []*enode.Node
	)
	go func() {
		var err error
		response, err = test.udp.findnode(remote, distances, 0)
		done <- err
	}()

	// Serve the responses:
	test.requirePacketOut(func(p *v5wire.Findnode, addr *net.UDPAddr, _ v5wire.Nonce) {
		if !reflect.DeepEqual(p.Distances, distances) {
			t.Fatalf("wrong distances in request: %v", p.Distances)
		}
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 2,
			Nodes:     nodesToRecords(nodes[:4]),
		})
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 2,
			Nodes:     nodesToRecords(nodes[4:]),
		})
	})

	// Check results:
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(response, nodes) {
		t.Fatalf("wrong nodes in response")
	}

	// TODO: check invalid IPs
	// TODO: check invalid/unsigned record
}

// This test checks that outgoing TOPICQUERY calls work.
func TestUDPv5_topicqueryCall(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	// Launch the request:
	var (
		topic       = topicindex.TopicID{1, 1, 1, 1}
		remote      = test.getNode(test.remotekey, test.remoteaddr).Node()
		resultNodes = nodesAtDistance(remote.ID(), 256, 8)
		auxNodes    = nodesAtDistance(remote.ID(), 255, 6)
		done        = make(chan error, 1)
		response    topicQueryResult
	)
	go func() {
		var err error
		response = test.udp.topicQuery(remote, topic, []uint{}, 0)
		done <- err
	}()

	// Serve the responses:
	test.requirePacketOut(func(p *v5wire.TopicQuery, addr *net.UDPAddr, _ v5wire.Nonce) {
		if p.Topic != topic {
			t.Fatalf("wrong topic in request: %v", p.Topic)
		}
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 4,
			Nodes:     nodesToRecords(auxNodes[:4]),
		})
		test.packetIn(&v5wire.TopicNodes{
			ReqID:     p.ReqID,
			RespCount: 4,
			Nodes:     nodesToRecords(resultNodes[:4]),
		})
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 4,
			Nodes:     nodesToRecords(auxNodes[4:]),
		})
		test.packetIn(&v5wire.TopicNodes{
			ReqID:     p.ReqID,
			RespCount: 4,
			Nodes:     nodesToRecords(resultNodes[4:]),
		})
	})

	// Check results:
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := checkNodesEqual(response.auxNodes, auxNodes); err != nil {
		t.Fatal("wrong auxNodes in response:", err)
	}
	if err := checkNodesEqual(response.topicNodes, resultNodes); err != nil {
		t.Fatal("wrong topicNodes in response:", err)
	}
}

// This test checks that pending calls are re-sent when a handshake happens.
func TestUDPv5_callResend(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan error, 2)
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()

	// Ping answered by WHOAREYOU.
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, nonce v5wire.Nonce) {
		test.packetIn(&v5wire.Whoareyou{Nonce: nonce})
	})
	// Ping should be re-sent.
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Pong{ReqID: p.ReqID})
	})
	// Answer the other ping.
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Pong{ReqID: p.ReqID})
	})
	if err := <-done; err != nil {
		t.Fatalf("unexpected ping error: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("unexpected ping error: %v", err)
	}
}

// This test ensures we don't allow multiple rounds of WHOAREYOU for a single call.
func TestUDPv5_multipleHandshakeRounds(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan error, 1)
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()

	// Ping answered by WHOAREYOU.
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, nonce v5wire.Nonce) {
		test.packetIn(&v5wire.Whoareyou{Nonce: nonce})
	})
	// Ping answered by WHOAREYOU again.
	test.requirePacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, nonce v5wire.Nonce) {
		test.packetIn(&v5wire.Whoareyou{Nonce: nonce})
	})
	if err := <-done; err != errTimeout {
		t.Fatalf("unexpected ping error: %q", err)
	}
}

// This test checks that calls with n replies may take up to n * respTimeout.
func TestUDPv5_callTimeoutReset(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	// Launch the request:
	var (
		distance = uint(230)
		remote   = test.getNode(test.remotekey, test.remoteaddr).Node()
		nodes    = nodesAtDistance(remote.ID(), int(distance), 8)
		done     = make(chan error, 1)
	)
	go func() {
		_, err := test.udp.findnode(remote, []uint{distance}, 0)
		done <- err
	}()

	// Serve two responses, slowly.
	test.requirePacketOut(func(p *v5wire.Findnode, addr *net.UDPAddr, _ v5wire.Nonce) {
		time.Sleep(respTimeout - 50*time.Millisecond)
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 2,
			Nodes:     nodesToRecords(nodes[:4]),
		})

		time.Sleep(respTimeout - 50*time.Millisecond)
		test.packetIn(&v5wire.Nodes{
			ReqID:     p.ReqID,
			RespCount: 2,
			Nodes:     nodesToRecords(nodes[4:]),
		})
	})
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %q", err)
	}
}

// This test checks that TALKREQ calls the registered handler function.
func TestUDPv5_talkHandling(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	var recvMessage []byte
	test.udp.RegisterTalkHandler("test", func(id enode.ID, addr *net.UDPAddr, message []byte) []byte {
		recvMessage = message
		return []byte("test response")
	})

	// Successful case:
	test.packetIn(&v5wire.TalkRequest{
		ReqID:    []byte("foo"),
		Protocol: "test",
		Message:  []byte("test request"),
	})
	test.requirePacketOut(func(p *v5wire.TalkResponse, addr *net.UDPAddr, _ v5wire.Nonce) {
		if !bytes.Equal(p.ReqID, []byte("foo")) {
			t.Error("wrong request ID in response:", p.ReqID)
		}
		if string(p.Message) != "test response" {
			t.Errorf("wrong talk response message: %q", p.Message)
		}
		if string(recvMessage) != "test request" {
			t.Errorf("wrong message received in handler: %q", recvMessage)
		}
	})

	// Check that empty response is returned for unregistered protocols.
	recvMessage = nil
	test.packetIn(&v5wire.TalkRequest{
		ReqID:    []byte("2"),
		Protocol: "wrong",
		Message:  []byte("test request"),
	})
	test.requirePacketOut(func(p *v5wire.TalkResponse, addr *net.UDPAddr, _ v5wire.Nonce) {
		if !bytes.Equal(p.ReqID, []byte("2")) {
			t.Error("wrong request ID in response:", p.ReqID)
		}
		if string(p.Message) != "" {
			t.Errorf("wrong talk response message: %q", p.Message)
		}
		if recvMessage != nil {
			t.Errorf("handler was called for wrong protocol: %q", recvMessage)
		}
	})
}

// This test checks that outgoing TALKREQ calls work.
func TestUDPv5_talkRequest(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan error, 1)

	// This request times out.
	go func() {
		_, err := test.udp.TalkRequest(remote, "test", []byte("test request"))
		done <- err
	}()
	test.requirePacketOut(func(p *v5wire.TalkRequest, addr *net.UDPAddr, _ v5wire.Nonce) {})
	if err := <-done; err != errTimeout {
		t.Fatalf("want errTimeout, got %q", err)
	}

	// This request works.
	go func() {
		_, err := test.udp.TalkRequest(remote, "test", []byte("test request"))
		done <- err
	}()
	test.requirePacketOut(func(p *v5wire.TalkRequest, addr *net.UDPAddr, _ v5wire.Nonce) {
		if p.Protocol != "test" {
			t.Errorf("wrong protocol ID in talk request: %q", p.Protocol)
		}
		if string(p.Message) != "test request" {
			t.Errorf("wrong message talk request: %q", p.Message)
		}
		test.packetInFrom(test.remotekey, test.remoteaddr, &v5wire.TalkResponse{
			ReqID:   p.ReqID,
			Message: []byte("test response"),
		})
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// This test checks that lookup works.
func TestUDPv5_lookup(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})

	// Lookup on empty table returns no nodes.
	if results := test.udp.Lookup(lookupTestnet.target.id()); len(results) > 0 {
		t.Fatalf("lookup on empty table returned %d results: %#v", len(results), results)
	}

	// Ensure the tester knows all nodes in lookupTestnet by IP.
	for d, nn := range lookupTestnet.dists {
		for i, key := range nn {
			n := lookupTestnet.node(d, i)
			test.getNode(key, &net.UDPAddr{IP: n.IP(), Port: n.UDP()})
		}
	}

	// Seed table with initial node.
	initialNode := lookupTestnet.node(256, 0)
	fillTable(test.table, []*node{wrapNode(initialNode)})

	// Start the lookup.
	resultC := make(chan []*enode.Node, 1)
	go func() {
		resultC <- test.udp.Lookup(lookupTestnet.target.id())
		test.close()
	}()

	// Answer lookup packets.
	asked := make(map[enode.ID]bool)
	for done := false; !done; {
		timeout := 3 * time.Second
		err := test.waitPacketOut(timeout, func(p v5wire.Packet, to *net.UDPAddr, _ v5wire.Nonce) {
			recipient, key := lookupTestnet.nodeByAddr(to)
			switch p := p.(type) {
			case *v5wire.Ping:
				test.packetInFrom(key, to, &v5wire.Pong{ReqID: p.ReqID})
			case *v5wire.Findnode:
				if asked[recipient.ID()] {
					t.Error("Asked node", recipient.ID(), "twice")
				}
				asked[recipient.ID()] = true
				nodes := lookupTestnet.neighborsAtDistances(recipient, p.Distances, 16)
				t.Logf("Got FINDNODE for %v, returning %d nodes", p.Distances, len(nodes))
				for _, resp := range packFindnodeResponse(p.ReqID, nodes) {
					test.packetInFrom(key, to, resp)
				}
			}
		})

		if err == errClosed {
			done = true
		} else if err != nil {
			t.Fatal("receive error:", err)
		}
	}

	// Verify result nodes.
	results := <-resultC
	checkLookupResults(t, lookupTestnet, results)
}

// This test checks the local node can be utilised to set key-values.
func TestUDPv5_LocalNode(t *testing.T) {
	t.Parallel()
	var cfg Config
	node := startLocalhostV5(t, cfg)
	defer node.Close()
	localNd := node.LocalNode()

	// set value in node's local record
	testVal := [4]byte{'A', 'B', 'C', 'D'}
	localNd.Set(enr.WithEntry("testing", &testVal))

	// retrieve the value from self to make sure it matches.
	outputVal := [4]byte{}
	if err := node.Self().Load(enr.WithEntry("testing", &outputVal)); err != nil {
		t.Errorf("Could not load value from record: %v", err)
	}
	if testVal != outputVal {
		t.Errorf("Wanted %#x to be retrieved from the record but instead got %#x", testVal, outputVal)
	}
}

func TestUDPv5_PingWithIPV4MappedAddress(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t, Config{})
	defer test.close()

	rawIP := net.IPv4(0xFF, 0x12, 0x33, 0xE5)
	test.remoteaddr = &net.UDPAddr{
		IP:   rawIP.To16(),
		Port: 0,
	}
	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan struct{}, 1)

	// This handler will truncate the ipv4-mapped in ipv6 address.
	go func() {
		test.udp.handlePing(&v5wire.Ping{ENRSeq: 1}, remote.ID(), test.remoteaddr)
		done <- struct{}{}
	}()
	test.requirePacketOut(func(p *v5wire.Pong, addr *net.UDPAddr, _ v5wire.Nonce) {
		if len(p.ToIP) == net.IPv6len {
			t.Error("Received untruncated ip address")
		}
		if len(p.ToIP) != net.IPv4len {
			t.Errorf("Received ip address with incorrect length: %d", len(p.ToIP))
		}
		if !p.ToIP.Equal(rawIP) {
			t.Errorf("Received incorrect ip address: wanted %s but received %s", rawIP.String(), p.ToIP.String())
		}
	})
	<-done
}

// packetsOfType returns all packets of type T from the given set.
func packetsOfType[T v5wire.Packet](ps []v5wire.Packet) (matches []T) {
	for _, p := range ps {
		if match, ok := p.(T); ok {
			matches = append(matches, match)
		}
	}
	return matches
}

// checkResponseCountField checks that ResponseCount == wantCount in the given packets.
func checkResponseCountField[T v5wire.Packet](t *testing.T, msgs []T, wantCount uint8) {
	t.Helper()

	for _, msg := range msgs {
		check := func(c uint8) {
			if c != wantCount {
				t.Fatalf("invalid response count %d in %T, want %d", c, msg, wantCount)
			}
		}
		switch msg := any(msg).(type) {
		case *v5wire.Nodes:
			check(msg.RespCount)
		case *v5wire.TopicNodes:
			check(msg.RespCount)
		case *v5wire.Regconfirmation:
			check(msg.RespCount)
		default:
		}
	}
}

// checkResponseNodes checks that the nodes contained in msgs are equal to the expected nodes.
// The ordering of expectedNodes is ignored.
func checkResponseNodes[T v5wire.Packet](t *testing.T, msgs []T, expectedNodes []*enode.Node) {
	t.Helper()

	nodes := responseNodes(msgs)
	sortByID(nodes)

	expectedNodes = append([]*enode.Node(nil), expectedNodes...)
	sortByID(expectedNodes)

	if err := checkNodesEqual(nodes, expectedNodes); err != nil {
		t.Fatal(err)
	}
}

// responseNodes returns all nodes contained in the given messages.
func responseNodes[T v5wire.Packet](msgs []T) []*enode.Node {
	var nodes []*enode.Node
	for _, msg := range msgs {
		var records []*enr.Record
		switch msg := any(msg).(type) {
		case *v5wire.Nodes:
			records = msg.Nodes
		case *v5wire.TopicNodes:
			records = msg.Nodes
		}
		nodes = append(nodes, recordsToNodes(enode.ValidSchemesForTesting, records)...)
	}
	return nodes
}

// udpV5Test is the framework for all tests above.
// It runs the UDPv5 transport on a virtual socket and allows testing outgoing packets.
type udpV5Test struct {
	t                   *testing.T
	pipe                *dgramPipe
	table               *Table
	db                  *enode.DB
	udp                 *UDPv5
	localkey, remotekey *ecdsa.PrivateKey
	remoteaddr          *net.UDPAddr
	nodesByID           map[enode.ID]*enode.LocalNode
	nodesByIP           map[string]*enode.LocalNode
}

// testCodec is the packet encoding used by protocol tests. This codec does not perform encryption.
type testCodec struct {
	test *udpV5Test
	id   enode.ID
	ctr  uint64
}

type testCodecFrame struct {
	NodeID  enode.ID
	AuthTag v5wire.Nonce
	Ptype   byte
	Packet  rlp.RawValue
}

func (c *testCodec) Encode(toID enode.ID, addr string, p v5wire.Packet, _ *v5wire.Whoareyou) ([]byte, v5wire.Nonce, error) {
	c.ctr++
	var authTag v5wire.Nonce
	binary.BigEndian.PutUint64(authTag[:], c.ctr)

	penc, _ := rlp.EncodeToBytes(p)
	frame, err := rlp.EncodeToBytes(testCodecFrame{c.id, authTag, p.Kind(), penc})
	return frame, authTag, err
}

func (c *testCodec) Decode(input []byte, addr string) (enode.ID, *enode.Node, v5wire.Packet, error) {
	frame, p, err := c.decodeFrame(input)
	if err != nil {
		return enode.ID{}, nil, nil, err
	}
	return frame.NodeID, nil, p, nil
}

func (c *testCodec) decodeFrame(input []byte) (frame testCodecFrame, p v5wire.Packet, err error) {
	if err = rlp.DecodeBytes(input, &frame); err != nil {
		return frame, nil, fmt.Errorf("invalid frame: %v", err)
	}
	switch frame.Ptype {
	case v5wire.UnknownPacket:
		dec := new(v5wire.Unknown)
		err = rlp.DecodeBytes(frame.Packet, &dec)
		p = dec
	case v5wire.WhoareyouPacket:
		dec := new(v5wire.Whoareyou)
		err = rlp.DecodeBytes(frame.Packet, &dec)
		p = dec
	default:
		p, err = v5wire.DecodeMessage(frame.Ptype, frame.Packet)
	}
	return frame, p, err
}

func newUDPV5Test(t *testing.T, cfg Config) *udpV5Test {
	test := &udpV5Test{
		t:          t,
		pipe:       newpipe(),
		localkey:   newkey(),
		remotekey:  newkey(),
		remoteaddr: &net.UDPAddr{IP: net.IP{10, 0, 1, 99}, Port: 30303},
		nodesByID:  make(map[enode.ID]*enode.LocalNode),
		nodesByIP:  make(map[string]*enode.LocalNode),
	}

	cfg.PrivateKey = test.localkey
	cfg.Log = testlog.Logger(t, log.LvlTrace)
	cfg.ValidSchemes = enode.ValidSchemesForTesting

	test.db, _ = enode.OpenDB("")
	ln := enode.NewLocalNode(test.db, test.localkey)
	ln.SetStaticIP(net.IP{10, 0, 0, 1})
	ln.SetFallbackUDP(30303)
	test.udp, _ = ListenV5(test.pipe, ln, cfg)
	test.udp.codec = &testCodec{test: test, id: ln.ID()}
	test.table = test.udp.tab
	test.nodesByID[ln.ID()] = ln
	// Wait for initial refresh so the table doesn't send unexpected findnode.
	<-test.table.initDone
	return test
}

// handles a packet as if it had been sent to the transport.
func (test *udpV5Test) packetIn(packet v5wire.Packet) {
	test.t.Helper()
	test.packetInFrom(test.remotekey, test.remoteaddr, packet)
}

// handles a packet as if it had been sent to the transport by the key/endpoint.
func (test *udpV5Test) packetInFrom(key *ecdsa.PrivateKey, addr *net.UDPAddr, packet v5wire.Packet) {
	test.t.Helper()

	ln := test.getNode(key, addr)
	codec := &testCodec{test: test, id: ln.ID()}
	enc, _, err := codec.Encode(test.udp.Self().ID(), addr.String(), packet, nil)
	if err != nil {
		test.t.Errorf("%s encode error: %v", packet.Name(), err)
	}
	if test.udp.dispatchReadPacket(addr, enc) {
		<-test.udp.readNextCh // unblock UDPv5.dispatch
	}
}

// getNode ensures the test knows about a node at the given endpoint.
func (test *udpV5Test) getNode(key *ecdsa.PrivateKey, addr *net.UDPAddr) *enode.LocalNode {
	id := encodePubkey(&key.PublicKey).id()
	ln := test.nodesByID[id]
	if ln == nil {
		db, _ := enode.OpenDB("")
		ln = enode.NewLocalNode(db, key)
		ln.SetStaticIP(addr.IP)
		ln.Set(enr.UDP(addr.Port))
		test.nodesByID[id] = ln
	}
	test.nodesByIP[string(addr.IP)] = ln
	return ln
}

// createNode generates a new test node. The IP address is assigned based on the given index.
func (test *udpV5Test) createNode(ipIndex int) (*ecdsa.PrivateKey, *enode.LocalNode) {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}
	ln := test.getNode(key, &net.UDPAddr{IP: intIP(ipIndex), Port: 30303})
	return key, ln
}

// expectNodes waits for n NODES responses and checks that they contain the given nodes.
// Order of wantNodes is not significant.
func (test *udpV5Test) expectNodes(n int, reqID []byte, wantNodes []*enode.Node) {
	test.t.Helper()

	resp := test.expectMultipleResponses(n, reqID, 3*time.Second)
	nodesMessages := packetsOfType[*v5wire.Nodes](resp)

	if len(nodesMessages) != len(resp) {
		test.t.Fatal("unexpected non-NODES responses")
	}
	checkResponseCountField(test.t, resp, uint8(n))
	checkResponseNodes(test.t, resp, wantNodes)
}

// expectMultipleResponses collects response packets to the given request ID.
func (test *udpV5Test) expectMultipleResponses(n int, reqID []byte, timeout time.Duration) []v5wire.Packet {
	var packets []v5wire.Packet
	start := time.Now()
	for i := 0; i < n; i++ {
		timeSpent := time.Since(start)
		if timeSpent >= timeout {
			return packets
		}
		waitTime := timeout - timeSpent

		err := test.waitPacketOut(waitTime, func(p v5wire.Packet, addr *net.UDPAddr, _ v5wire.Nonce) {
			if !bytes.Equal(p.RequestID(), reqID) {
				test.t.Fatalf("wrong request ID %v in response, want %v", p.RequestID(), reqID)
			}
			packets = append(packets, p)
		})
		if err == errClosed {
			test.t.Fatal("socket closed while waiting for response")
		}
	}
	if len(packets) != n {
		test.t.Errorf("invalid number of responses: got %d, want %d", len(packets), n)
	}
	return packets
}

// packetMatchFunc is a packet validator function. It must be
// of type func (X, *net.UDPAddr, v5wire.Nonce) where X is assignable to v5wire.Packet.
type packetMatchFunc interface{}

func packetMatchFuncExpectedType(fn packetMatchFunc) reflect.Type {
	return reflect.ValueOf(fn).Type().In(0)
}

// requirePacketOut waits for the next output packet and handles it using the given
// validation function.
func (test *udpV5Test) requirePacketOut(fn packetMatchFunc) {
	test.t.Helper()

	exptype := packetMatchFuncExpectedType(fn)
	err := test.waitPacketOut(3*time.Second, fn)
	switch err {
	case nil:
	case errTimeout:
		test.t.Fatalf("timed out waiting for %v", exptype)
	case errClosed:
		test.t.Fatalf("socket closed while waiting for %v", exptype)
	default:
		test.t.Fatalf("receive error: %v", err)
	}
}

// waitPacketOut waits for a packet to be sent. When a packet arrives, the validation
// function is called to handle it. The returned error is

// - nil if validate was called.
// - errTimeout if no packet arrived within the timeout.
// - errClosed if the socket was closed.
func (test *udpV5Test) waitPacketOut(timeout time.Duration, validate packetMatchFunc) error {
	test.t.Helper()

	dgram, err := test.pipe.receive(3 * time.Second)
	if err != nil {
		return err
	}
	test.validatePacketOut(validate, dgram)
	return nil
}

// validateOutputPacket decodes a packet and calls validateFunc to check it.
func (test *udpV5Test) validatePacketOut(validate packetMatchFunc, dgram dgram) {
	exptype := packetMatchFuncExpectedType(validate)

	ln := test.nodesByIP[string(dgram.to.IP)]
	if ln == nil {
		test.t.Fatalf("attempt to send to non-existing node %v", &dgram.to)
		return
	}
	codec := &testCodec{test: test, id: ln.ID()}
	frame, p, err := codec.decodeFrame(dgram.data)
	if err != nil {
		test.t.Fatalf("sent packet decode error: %v", err)
		return
	}
	if !reflect.TypeOf(p).AssignableTo(exptype) {
		test.t.Fatalf("sent packet type mismatch, got: %v, want: %v", reflect.TypeOf(p), exptype)
		return
	}
	fn := reflect.ValueOf(validate)
	fn.Call([]reflect.Value{reflect.ValueOf(p), reflect.ValueOf(&dgram.to), reflect.ValueOf(frame.AuthTag)})
}

func (test *udpV5Test) close() {
	test.t.Helper()

	test.udp.Close()
	test.db.Close()
	for id, n := range test.nodesByID {
		if id != test.udp.Self().ID() {
			n.Database().Close()
		}
	}
	if len(test.pipe.queue) != 0 {
		test.t.Fatalf("%d unmatched UDP packets in queue", len(test.pipe.queue))
	}
}
