package test

import (
	"crypto/ecdsa"
	"flag"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

const (
	expiration = 20 * time.Second
	sigSize    = 520 / 8
)

var (
	enodeID           string
	remoteAddr        string
	toID              enode.ID
	toAddr            *net.UDPAddr
	priv              *ecdsa.PrivateKey
	versionPrefix     = []byte("v4")
	versionPrefixSize = len(versionPrefix)
	headSize          = versionPrefixSize + sigSize // space of packet frame data
	conn              *net.UDPConn
)

func init() {
	flag.StringVar(&enodeID, "enode", "", "enode:... as per `admin.nodeInfo.enode`")
	flag.StringVar(&remoteAddr, "remoteAddr", "127.0.0.1:30303", "")

	var err error
	priv, err = crypto.GenerateKey()
	if err != nil {
		panic(err)
	}

	raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
}

//ripped out from the urlv4 code
func signV4Compat(r *enr.Record, pubkey *ecdsa.PublicKey) {
	r.Set((*enode.Secp256k1)(pubkey))
	if err := r.SetSig(v4CompatID{}, []byte{}); err != nil {
		panic(err)
	}
}

type v4CompatID struct {
	enode.V4ID
}

type MacENREntry string

func (v MacENREntry) ENRKey() string { return "mac" }

func futureExpiration() uint64 {
	return uint64(time.Now().Add(expiration).Unix())
}

func MakeNode(pubkey *ecdsa.PublicKey, ip net.IP, tcp, udp int, mac *string) *enode.Node {
	var r enr.Record
	if ip != nil {
		r.Set(enr.IP(ip))
	}
	if udp != 0 {
		r.Set(enr.UDP(udp))
	}
	if tcp != 0 {
		r.Set(enr.TCP(tcp))
	}
	if mac != nil {
		r.Set(MacENREntry(*mac))
	}

	signV4Compat(&r, pubkey)
	n, err := enode.New(v4CompatID{}, &r)
	if err != nil {
		panic(err)
	}
	return n
}

var headSpace = make([]byte, headSize)

func sendPacket(packet []byte) (v4wire.Packet, error) {
	raddr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	n, err := conn.Write(packet)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 2048)
	n, err = conn.Read(buf)
	if err != nil {
		return nil, err
	}
	p, _, _, err := v4wire.Decode(buf[:n])
	if err != nil {
		return nil, err
	}
	return p, nil
}

func SimplePing(t *testing.T) {
	ipAddr := net.ParseIP("127.0.0.1")
	req := v4wire.Ping{
		Version:    4,
		From:       v4wire.Endpoint{IP: ipAddr},
		To:         v4wire.Endpoint{IP: ipAddr},
		Expiration: futureExpiration(),
	}
	packet, _, err := v4wire.Encode(priv, &req)
	if err != nil {
		t.Error("Encoding", err)
	}

	reply, err := sendPacket(packet)
	if err != nil {
		t.Error("Sending", err)
	}
	if reply.Kind() != v4wire.PongPacket {
		t.Error("Reply is not a Pong", reply.Name())
	}
}

func SourceUnknownPingKnownEnode(t *testing.T)          {}
func SourceUnknownPingWrongTo(t *testing.T)             {}
func SourceUnknownPingWrongFrom(t *testing.T)           {}
func SourceUnknownPingExtraData(t *testing.T)           {}
func SourceUnknownPingExtraDataWrongFrom(t *testing.T)  {}
func SourceUnknownWrongPacketType(t *testing.T)         {}
func SourceUnknownFindNeighbours(t *testing.T)          {}
func SourceKnownPingFromSignatureMismatch(t *testing.T) {}
func PingPastExpiration(t *testing.T)                   {}

func SpoofSanityCheck(t *testing.T)              {}
func SpoofAmplificationAttackCheck(t *testing.T) {}

func FindNeighboursOnRecentlyBondedTarget(t *testing.T) {}
func FindNeighboursPastExpiration(t *testing.T)         {}

func TestPing(t *testing.T) {
	t.Run("Ping-Simple", SimplePing)
	t.Run("Ping-BasicTest(v4001)", SourceUnknownPingKnownEnode)
	t.Run("Ping-SourceUnknownrongTo(v4002)", SourceUnknownPingWrongTo)
	t.Run("Ping-SourceUnknownWrongFrom(v4003)", SourceUnknownPingWrongFrom)
	t.Run("Ping-SourceUnknownExtraData(v4004)", SourceUnknownPingExtraData)
	t.Run("Ping-SourceUnknownExtraDataWrongFrom(v4005)", SourceUnknownPingExtraDataWrongFrom)
	t.Run("Ping-SourceUnknownWrongPacketType(v4006)", SourceUnknownWrongPacketType)
	t.Run("Ping-BondedFromSignatureMismatch(v4009)", SourceKnownPingFromSignatureMismatch)
	t.Run("Ping-PastExpiration(v4011)", PingPastExpiration)
}

func TestSpoofing(t *testing.T) {
	t.Run("SpoofSanityCheck(v4013)", SpoofSanityCheck)
	t.Run("SpoofAmplification(v4014)", SpoofAmplificationAttackCheck)
}

func TestFindNode(t *testing.T) {
	t.Run("Findnode-UnbondedFindNeighbours(v4007)", SourceUnknownFindNeighbours)
	t.Run("FindNode-UnsolicitedPollution(v4010)", FindNeighboursOnRecentlyBondedTarget)
	t.Run("FindNode-PastExpiration(v4012)", FindNeighboursPastExpiration)
}
