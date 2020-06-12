package test

import (
	"crypto/rand"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
)

const (
	expiration  = 20 * time.Second
	wrongPacket = 66
	macSize     = 256 / 8
)

var (
	enodeID  = flag.String("enode", "", "enode:... as per `admin.nodeInfo.enode`")
	remote   = flag.String("remote", "127.0.0.1:30303", "")
	waitTime = flag.Int("waitTime", 500, "ms to wait for response")
)

type pingWithJunk struct {
	Version    uint
	From, To   v4wire.Endpoint
	Expiration uint64
	JunkData1  uint
	JunkData2  []byte
}

func (req *pingWithJunk) Name() string { return "PING/v4" }
func (req *pingWithJunk) Kind() byte   { return v4wire.PingPacket }

type pingWrongType struct {
	Version    uint
	From, To   v4wire.Endpoint
	Expiration uint64
}

func (req *pingWrongType) Name() string { return "WRONG/v4" }
func (req *pingWrongType) Kind() byte   { return wrongPacket }

func TestMain(m *testing.M) {
	if os.Getenv("CI") != "" {
		os.Exit(0)
	}
	flag.Parse()
	os.Exit(m.Run())
}

func futureExpiration() uint64 {
	return uint64(time.Now().Add(expiration).Unix())
}

// This test just sends a PING packet and expects a response.
func PingKnownEnode(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	req := v4wire.Ping{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

// This test sends a PING packet with wrong 'To' field and expects a PONG response.
func PingWrongTo(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	wrongEndpoint := v4wire.Endpoint{IP: net.ParseIP("192.0.2.0")}
	req := v4wire.Ping{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         wrongEndpoint,
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

// This test sends a PING packet with wrong 'From' field and expects a PONG response.
func PingWrongFrom(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	wrongEndpoint := v4wire.Endpoint{IP: net.ParseIP("192.0.2.0")}
	req := v4wire.Ping{
		Version:    4,
		From:       wrongEndpoint,
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

// This test sends a PING packet with additional data at the end and expects a PONG
// response. The remote node should respond because EIP-8 mandates ignoring additional
// trailing data.
func PingExtraData(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	req := pingWithJunk{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
		JunkData1:  42,
		JunkData2:  []byte{9, 8, 7, 6, 5, 4, 3, 2, 1},
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

// This test sends a PING packet with additional data and wrong 'from' field
// and expects a PONG response.
func PingExtraDataWrongFrom(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	wrongEndpoint := v4wire.Endpoint{IP: net.ParseIP("192.0.2.0")}
	req := pingWithJunk{
		Version:    4,
		From:       wrongEndpoint,
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
		JunkData1:  42,
		JunkData2:  []byte{9, 8, 7, 6, 5, 4, 3, 2, 1},
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

// This test sends a PING packet with an expiration in the past.
// The remote node should not respond.
func PingPastExpiration(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	req := v4wire.Ping{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         te.remoteEndpoint(),
		Expiration: -futureExpiration(),
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, _ := te.read(te.l1)
	if reply != nil {
		t.Fatal("Expected no reply, got", reply)
	}
}

// This test sends an invalid packet. The remote node should not respond.
func WrongPacketType(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	req := pingWrongType{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("send", err)
	}
	reply, _, _ := te.read(te.l1)
	if reply != nil {
		t.Fatal("Expected no reply, got", reply)
	}
}

// This test verifies that the default behaviour of ignoring 'from' fields is unaffected by
// the bonding process. After bonding, it pings the target with a different from endpoint.
func BondThenPingWithWrongFrom(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	bond(t, te)

	wrongEndpoint := v4wire.Endpoint{IP: net.ParseIP("192.0.2.0")}
	req2 := v4wire.Ping{
		Version:    4,
		From:       wrongEndpoint,
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &req2); err != nil {
		t.Fatal("send 2nd", err)
	}

	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read 2nd", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong after bonding", reply.Name())
	}
}

// This test just sends FINDNODE. The remote node should not reply
// because the endpoint proof has not completed.
func FindnodeWithoutEndpointProof(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()

	req := v4wire.Findnode{Expiration: futureExpiration()}
	rand.Read(req.Target[:])
	if err := te.send(te.l1, &req); err != nil {
		t.Fatal("sending find nodes", err)
	}
	reply, _, _ := te.read(te.l1)
	if reply != nil {
		t.Fatal("Expected no response, got", reply)
	}
}

// BasicFindnode sends a FINDNODE request after performing the endpoint
// proof. The remote node should respond.
func BasicFindnode(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()
	bond(t, te)

	//now call find neighbours
	findnode := v4wire.Findnode{Expiration: futureExpiration()}
	rand.Read(findnode.Target[:])
	if err := te.send(te.l1, &findnode); err != nil {
		t.Fatal("sending findnode", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read find nodes", err)
	}
	if reply.Kind() != v4wire.NeighborsPacket {
		t.Fatal("Expected neighbors, got", reply.Name())
	}
}

// This test sends an unsolicited NEIGHBORS packet after the endpoint proof, then sends
// FINDNODE to read the remote table. The remote node should not return the node contained
// in the unsolicited NEIGHBORS packet.
func UnsolicitedNeighbors(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()
	bond(t, te)

	// Send unsolicited NEIGHBORS response.
	fakeKey, _ := crypto.GenerateKey()
	encFakeKey := v4wire.EncodePubkey(&fakeKey.PublicKey)
	neighbors := v4wire.Neighbors{
		Expiration: futureExpiration(),
		Nodes: []v4wire.Node{{
			ID:  encFakeKey,
			IP:  net.IP{1, 2, 3, 4},
			UDP: 30303,
			TCP: 30303,
		}},
	}
	if err := te.send(te.l1, &neighbors); err != nil {
		t.Fatal("NeighborsReq", err)
	}

	// Check if the remote node included the fake node.
	findnode := v4wire.Findnode{
		Expiration: futureExpiration(),
		Target:     encFakeKey,
	}
	if err := te.send(te.l1, &findnode); err != nil {
		t.Fatal("sending findnode", err)
	}
	reply, _, err := te.read(te.l1)
	if err != nil {
		t.Fatal("read find nodes", err)
	}
	if reply.Kind() != v4wire.NeighborsPacket {
		t.Fatal("Expected neighbors, got", reply.Name())
	}
	nodes := reply.(*v4wire.Neighbors).Nodes
	if contains(nodes, encFakeKey) {
		t.Fatal("neighbors response contains node from earlier unsolicited neighbors response")
	}
}

// This test sends FINDNODE with an expiration timestamp in the past.
// The remote node should not respond.
func FindnodePastExpiration(t *testing.T) {
	te := newTestEnv(*remote)
	defer te.close()
	bond(t, te)

	findnode := v4wire.Findnode{
		Expiration: -futureExpiration(),
	}
	rand.Read(findnode.Target[:])
	if err := te.send(te.l1, &findnode); err != nil {
		t.Fatal("sending find nodes", err)
	}
	reply, _, _ := te.read(te.l1)
	if reply != nil {
		t.Fatal("Expected no reply, got", reply)
	}
}

// bond performs the endpoint proof with the remote node.
func bond(t *testing.T, te *testenv) {
	ping := v4wire.Ping{
		Version:    4,
		From:       te.localEndpoint(te.l1),
		To:         te.remoteEndpoint(),
		Expiration: futureExpiration(),
	}
	if err := te.send(te.l1, &ping); err != nil {
		t.Fatal("ping failed", err)
	}
	for {
		req, hash, err := te.read(te.l1)
		if err != nil {
			t.Fatal(err)
		}
		switch req.(type) {
		case *v4wire.Ping:
			te.send(te.l1, &v4wire.Pong{
				To:         te.remoteEndpoint(),
				ReplyTok:   hash,
				Expiration: futureExpiration(),
			})
			return
		case *v4wire.Pong:
			// TODO: maybe verify pong data here
			continue
		}
	}

}

// func SpoofSanityCheck(t *testing.T) {
// 	var err error
// 	var reply v4wire.Packet
//
// 	conn, err := net.ListenPacket("udp", "127.0.0.2:0")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer conn.Close()
//
// 	req := v4wire.Ping{
// 		Version:    4,
// 		From:       localhostEndpoint,
// 		To:         remoteEndpoint,
// 		Expiration: futureExpiration(),
// 	}
// 	if err := writeToPacketConn(conn, &req, remoteAddr); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	// We expect the relayConn to receive a pong
// 	reply, err = readFromPacketConn(conn)
// 	if err != nil {
// 		t.Fatal("read", err)
// 	}
// 	if reply.Kind() != v4wire.PongPacket {
// 		t.Error("Reply is not a Pong", reply.Name())
// 	}
// }
//
// // spoofed ping victim -> target
// // (pong target -> victim)
// // (ping target -> victim)
// // wait
// // spoofed pong victim -> target
// // spoofed findnode victim -> target
// // (target should ignore it)
// func SpoofAmplificationAttackCheck(t *testing.T) {
// 	var err error
//
// 	victimConn, err := net.ListenPacket("udp", "127.0.0.2:0")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer victimConn.Close()
//
// 	// send ping
// 	pingReq := v4wire.Ping{
// 		Version:    4,
// 		From:       localhostEndpoint,
// 		To:         remoteEndpoint,
// 		Expiration: futureExpiration(),
// 	}
// 	if err := writeToPacketConn(victimConn, &pingReq, remoteAddr); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	//wait for a tiny bit
// 	//NB: in a real scenario the 'victim' will have responded with a v4 pong
// 	//message to our ping recipient. in the attack scenario, the pong
// 	//will have been ignored because the source id is different than
// 	//expected. (to be more authentic, an improvement to this test
// 	//could be to send a fake pong from the node id - but this is not
// 	//essential because the following pong may be received prior to the
// 	//real pong)
// 	time.Sleep(200 * time.Millisecond)
//
// 	//send spoofed pong from this node id but with junk replytok
// 	//because the replytok will not be available to a real attacker
// 	//TODO- send a best reply tok guess?
// 	pongReq := &v4wire.Pong{
// 		To:         remoteEndpoint,
// 		ReplyTok:   make([]byte, macSize),
// 		Expiration: futureExpiration(),
// 	}
// 	if err := writeToPacketConn(victimConn, pongReq, remoteAddr); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	//consider the target 'bonded' , as it has received the expected pong
// 	//send a findnode request for a random 'target' (target there being the
// 	//node to find)
// 	var fakeKey *ecdsa.PrivateKey
// 	if fakeKey, err = crypto.GenerateKey(); err != nil {
// 		t.Fatal(err)
// 	}
// 	fakePub := fakeKey.PublicKey
// 	lookupTarget := v4wire.EncodePubkey(&fakePub)
//
// 	findReq := &v4wire.Findnode{
// 		Target:     lookupTarget,
// 		Expiration: futureExpiration(),
// 	}
// 	if err := writeToPacketConn(victimConn, findReq, remoteAddr); err != nil {
// 		t.Fatal(err)
// 	}
//
// 	// read a pong
// 	readFromPacketConn(victimConn)
// 	// read a ping
// 	readFromPacketConn(victimConn)
// 	//if we receive a neighbours request, then the attack worked and the test should fail
// 	reply, err := readFromPacketConn(victimConn)
// 	if reply != nil && reply.Kind() == v4wire.NeighborsPacket {
// 		t.Error("Got neighbors")
// 	}
// }
//
//

func TestPing(t *testing.T) {
	t.Run("Ping-BasicTest(v4001)", PingKnownEnode)
	t.Run("Ping-WrongTo(v4002)", PingWrongTo)
	t.Run("Ping-WrongFrom(v4003)", PingWrongFrom)
	t.Run("Ping-ExtraData(v4004)", PingExtraData)
	t.Run("Ping-ExtraDataWrongFrom(v4005)", PingExtraDataWrongFrom)
	t.Run("Ping-PastExpiration(v4011)", PingPastExpiration)
	t.Run("Ping-WrongPacketType(v4006)", WrongPacketType)
	t.Run("Ping-BondedFromSignatureMismatch(v4009)", BondThenPingWithWrongFrom)
}

//
// func TestSpoofing(t *testing.T) {
// 	t.Run("SpoofSanityCheck(v4013)", SpoofSanityCheck)
// 	t.Run("SpoofAmplification(v4014)", SpoofAmplificationAttackCheck)
// }
//

func TestFindNode(t *testing.T) {
	t.Run("Findnode-UnbondedFindNeighbours(v4007)", FindnodeWithoutEndpointProof)
	t.Run("Findnode-BasicFindnode(v4010)", BasicFindnode)
	t.Run("FindNode-UnsolicitedPollution(v4010)", UnsolicitedNeighbors)
	t.Run("FindNode-PastExpiration(v4012)", FindnodePastExpiration)
}
