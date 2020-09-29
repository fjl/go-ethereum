// Copyright 2019 The go-ethereum Authors
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

package v5wire

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/hkdf"
)

// TODO concurrent WHOAREYOU tie-breaker
// TODO deal with WHOAREYOU amplification factor (min packet size?)
// TODO add counter to nonce
// TODO rehandshake after X packets

// Discovery v5 packet structures.
type (
	packetHeader struct {
		ProtocolID [8]byte
		SrcID      enode.ID
		Flag       byte
		AuthSize   uint16
	}

	whoareyouAuthData struct {
		Nonce     Nonce    // nonce of request packet
		IDNonce   [32]byte // ID proof data
		RecordSeq uint64   // highest known ENR sequence of requester
	}

	handshakeAuthData struct {
		h struct {
			Nonce      Nonce // AES-GCM nonce of message
			SigSize    byte  // ignature data
			PubkeySize byte  // offset of
		}
		// Trailing variable-size data.
		signature, pubkey, record []byte
	}

	messageAuthData struct {
		Nonce Nonce // AES-GCM nonce of message
	}
)

// Packet header flag values.
const (
	flagMessage   = 0
	flagWhoareyou = 1
	flagHandshake = 2
)

// Protocol constants.
const (
	handshakeTimeout = time.Second
	handshakeVersion = 1
	minVersion       = 1
	protocolID       = "discv5"
	versionOffset    = 7
)

// Packet sizes.
var (
	sizeofMaskingIV           = 16
	sizeofPacketHeaderV5      = binary.Size(packetHeader{})
	sizeofWhoareyouAuthDataV5 = binary.Size(whoareyouAuthData{})
	sizeofHandshakeAuthDataV5 = binary.Size(handshakeAuthData{}.h)
	sizeofMessageAuthDataV5   = binary.Size(messageAuthData{})
)

// Errors.
var (
	errTooShort            = errors.New("packet too short")
	errInvalidHeader       = errors.New("invalid packet header")
	errInvalidFlag         = errors.New("invalid flag value in header")
	errUnexpectedHandshake = errors.New("unexpected auth response, not in handshake")
	errInvalidAuthKey      = errors.New("invalid ephemeral pubkey")
	errNoRecord            = errors.New("expected ENR in handshake but none sent")
	errInvalidNonceSig     = errors.New("invalid ID nonce signature")
	errMessageTooShort     = errors.New("message contains no data")
	errMessageDecrypt      = errors.New("cannot decrypt message")
)

// Codec encodes and decodes discovery v5 packets.
type Codec struct {
	sha256    hash.Hash
	localnode *enode.LocalNode
	privkey   *ecdsa.PrivateKey
	buf       bytes.Buffer // used for encoding of packets
	msgbuf    bytes.Buffer // used for encoding of message content
	reader    bytes.Reader // used for decoding
	sc        *SessionCache
}

// NewCodec creates a wire codec.
func NewCodec(ln *enode.LocalNode, key *ecdsa.PrivateKey, clock mclock.Clock) *Codec {
	c := &Codec{
		sha256:    sha256.New(),
		localnode: ln,
		privkey:   key,
		sc:        NewSessionCache(1024, clock),
	}
	return c
}

// Encode encodes a packet to a node. 'id' and 'addr' specify the destination node. The
// 'challenge' parameter should be the most recently received WHOAREYOU packet from that
// node.
func (c *Codec) Encode(id enode.ID, addr string, packet Packet, challenge *Whoareyou) ([]byte, Nonce, error) {
	if p, ok := packet.(*Whoareyou); ok {
		enc, err := c.encodeWhoareyou(id, p)
		if err == nil {
			c.sc.storeSentHandshake(id, addr, p)
		}
		return enc, Nonce{}, err
	}

	if challenge != nil {
		return c.encodeHandshakeMessage(id, addr, packet, challenge)
	}
	if session := c.sc.session(id, addr); session != nil {
		return c.encodeMessage(id, session, packet)
	}
	// No keys, no handshake: send random data to kick off the handshake.
	return c.encodeRandom(id)
}

// makeHeader creates a packet header.
func (c *Codec) makeHeader(toID enode.ID, flag byte, authsizeExtra int) *packetHeader {
	h := &packetHeader{
		SrcID: c.localnode.ID(),
		Flag:  flag,
	}
	var authsize int
	switch flag {
	case flagMessage:
		authsize = sizeofMessageAuthDataV5
	case flagWhoareyou:
		authsize = sizeofWhoareyouAuthDataV5
	case flagHandshake:
		authsize = sizeofHandshakeAuthDataV5
	default:
		panic(fmt.Errorf("BUG: invalid packet header flag %x", flag))
	}
	authsize += authsizeExtra
	if authsize > int(^uint16(0)) {
		panic(fmt.Errorf("BUG: auth size %d overflows uint16", authsize))
	}
	h.AuthSize = uint16(authsize)
	copy(h.ProtocolID[:], protocolID)
	h.ProtocolID[versionOffset] = handshakeVersion
	return h
}

// encodeRandom encodes a packet with random content.
func (c *Codec) encodeRandom(toID enode.ID) ([]byte, Nonce, error) {
	var auth messageAuthData
	if _, err := crand.Read(auth.Nonce[:]); err != nil {
		return nil, auth.Nonce, fmt.Errorf("can't get random data: %v", err)
	}

	c.buf.Reset()
	binary.Write(&c.buf, binary.BigEndian, c.makeHeader(toID, flagMessage, 0))
	binary.Write(&c.buf, binary.BigEndian, &auth)
	output := c.maskOutputPacket(toID, c.buf.Bytes(), c.buf.Len())
	return output, auth.Nonce, nil
}

// encodeWhoareyou encodes a WHOAREYOU packet.
func (c *Codec) encodeWhoareyou(toID enode.ID, packet *Whoareyou) ([]byte, error) {
	// Sanity check node field to catch misbehaving callers.
	if packet.RecordSeq > 0 && packet.Node == nil {
		panic("BUG: missing node in whoareyouV5 with non-zero seq")
	}
	auth := &whoareyouAuthData{
		Nonce:     packet.AuthTag,
		IDNonce:   packet.IDNonce,
		RecordSeq: packet.RecordSeq,
	}
	head := c.makeHeader(toID, flagWhoareyou, 0)

	c.buf.Reset()
	binary.Write(&c.buf, binary.BigEndian, head)
	binary.Write(&c.buf, binary.BigEndian, auth)
	output := c.maskOutputPacket(toID, c.buf.Bytes(), c.buf.Len())
	return output, nil
}

// encodeHandshakeMessage encodes an encrypted message with a handshake
// response header.
func (c *Codec) encodeHandshakeMessage(toID enode.ID, addr string, packet Packet, challenge *Whoareyou) ([]byte, Nonce, error) {
	// Ensure calling code sets challenge.node.
	if challenge.Node == nil {
		panic("BUG: missing challenge.Node in encode")
	}

	// Generate new secrets.
	auth, session, err := c.makeHandshakeHeader(toID, addr, challenge)
	if err != nil {
		return nil, Nonce{}, err
	}

	// TODO: this should happen when the first authenticated message is received
	c.sc.storeNewSession(toID, addr, session)

	// Encode header and auth header.
	var (
		authsizeExtra = len(auth.pubkey) + len(auth.signature) + len(auth.record)
		head          = c.makeHeader(toID, flagHandshake, authsizeExtra)
	)
	c.buf.Reset()
	binary.Write(&c.buf, binary.BigEndian, head)
	binary.Write(&c.buf, binary.BigEndian, &auth.h)
	c.buf.Write(auth.signature)
	c.buf.Write(auth.pubkey)
	c.buf.Write(auth.record)
	output := c.buf.Bytes()

	// Encrypt packet body.
	c.msgbuf.Reset()
	c.msgbuf.WriteByte(packet.Kind())
	if err := rlp.Encode(&c.msgbuf, packet); err != nil {
		return nil, auth.h.Nonce, err
	}
	messagePT := c.msgbuf.Bytes()
	headerData := output
	output, err = encryptGCM(output, session.writeKey, auth.h.Nonce[:], messagePT, headerData)
	if err == nil {
		output = c.maskOutputPacket(toID, output, len(headerData))
	}
	return output, auth.h.Nonce, err
}

// encodeAuthHeader creates the auth header on a call packet following WHOAREYOU.
func (c *Codec) makeHandshakeHeader(toID enode.ID, addr string, challenge *Whoareyou) (*handshakeAuthData, *session, error) {
	session := new(session)
	nonce, err := c.sc.nextNonce(session)
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate nonce: %v", err)
	}

	auth := new(handshakeAuthData)
	auth.h.Nonce = nonce

	// Create the ephemeral key. This needs to be first because the
	// key is part of the ID nonce signature.
	var remotePubkey = new(ecdsa.PublicKey)
	if err := challenge.Node.Load((*enode.Secp256k1)(remotePubkey)); err != nil {
		return nil, nil, fmt.Errorf("can't find secp256k1 key for recipient")
	}
	ephkey, err := c.sc.ephemeralKeyGen()
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate ephemeral key")
	}
	ephpubkey := EncodePubkey(&ephkey.PublicKey)
	auth.pubkey = ephpubkey[:]
	auth.h.PubkeySize = byte(len(auth.pubkey))

	// Add ID nonce signature to response.
	idsig, err := makeIDSignature(c.sha256, c.privkey, toID, challenge.IDNonce[:], ephpubkey[:])
	if err != nil {
		return nil, nil, fmt.Errorf("can't sign: %v", err)
	}
	auth.signature = idsig
	auth.h.SigSize = byte(len(auth.signature))

	// Add our record to response if it's newer than what remote
	// side has.
	ln := c.localnode.Node()
	if challenge.RecordSeq < ln.Seq() {
		auth.record, _ = rlp.EncodeToBytes(ln.Record())
	}

	// Create session keys.
	sec := c.deriveKeys(c.localnode.ID(), challenge.Node.ID(), ephkey, remotePubkey, challenge)
	if sec == nil {
		return nil, nil, fmt.Errorf("key derivation failed")
	}
	return auth, sec, err
}

// encodeMessage encodes an encrypted message packet.
func (c *Codec) encodeMessage(toID enode.ID, s *session, packet Packet) ([]byte, Nonce, error) {
	var (
		head = c.makeHeader(toID, flagMessage, 0)
		auth messageAuthData
	)

	// Create the nonce.
	nonce, err := c.sc.nextNonce(s)
	if err != nil {
		return nil, auth.Nonce, fmt.Errorf("can't generate nonce: %v", err)
	}
	auth.Nonce = nonce

	// Encode the header.
	c.buf.Reset()
	binary.Write(&c.buf, binary.BigEndian, head)
	binary.Write(&c.buf, binary.BigEndian, &auth)
	output := c.buf.Bytes()

	// Encode the message plaintext.
	c.msgbuf.Reset()
	c.msgbuf.WriteByte(packet.Kind())
	if err := rlp.Encode(&c.msgbuf, packet); err != nil {
		return nil, auth.Nonce, err
	}
	messagePT := c.msgbuf.Bytes()

	// Encrypt the message.
	headerData := output
	output, err = encryptGCM(output, s.writeKey, nonce[:], messagePT, headerData)
	if err == nil {
		output = c.maskOutputPacket(toID, output, len(headerData))
	}
	return output, auth.Nonce, err
}

// Decode decodes a discovery packet.
func (c *Codec) Decode(input []byte, addr string) (src enode.ID, n *enode.Node, p Packet, err error) {
	// Delete timed-out handshakes. This must happen before decoding to avoid
	// processing the same handshake twice.
	c.sc.handshakeGC()

	// Unmask the header.
	if len(input) < sizeofPacketHeaderV5+sizeofMaskingIV {
		return enode.ID{}, nil, nil, errTooShort
	}
	mask := headerMask(c.localnode.ID(), input)
	input = input[sizeofMaskingIV:]
	headerData := input[:sizeofPacketHeaderV5]
	mask.XORKeyStream(headerData, headerData)

	// Decode and verify the header.
	var head packetHeader
	c.reader.Reset(input)
	binary.Read(&c.reader, binary.BigEndian, &head)
	if !head.isValid(c.reader.Len()) {
		return enode.ID{}, nil, nil, errInvalidHeader
	}
	src = head.SrcID

	// Unmask auth data.
	authData := input[sizeofPacketHeaderV5 : sizeofPacketHeaderV5+int(head.AuthSize)]
	mask.XORKeyStream(authData, authData)

	// Decode auth part and message.
	switch head.Flag {
	case flagWhoareyou:
		p, err = c.decodeWhoareyou(&head)
	case flagHandshake:
		n, p, err = c.decodeHandshakeMessage(addr, &head, input)
	case flagMessage:
		p, err = c.decodeMessage(addr, &head, input)
	default:
		err = errInvalidFlag
	}
	return src, n, p, err
}

func (h *packetHeader) isValid(packetLen int) bool {
	for i := range protocolID {
		if h.ProtocolID[i] != protocolID[i] {
			return false
		}
	}
	if h.ProtocolID[versionOffset] < minVersion {
		return false
	}
	return int(h.AuthSize) <= packetLen
}

// decodeWhoareyou reads packet data after the header as a WHOAREYOU packet.
func (c *Codec) decodeWhoareyou(head *packetHeader) (Packet, error) {
	if c.reader.Len() < sizeofWhoareyouAuthDataV5 {
		return nil, errTooShort
	}
	if int(head.AuthSize) != sizeofWhoareyouAuthDataV5 {
		return nil, fmt.Errorf("invalid auth size for whoareyou")
	}
	auth := new(whoareyouAuthData)
	binary.Read(&c.reader, binary.BigEndian, auth)
	p := &Whoareyou{
		AuthTag:   auth.Nonce,
		IDNonce:   auth.IDNonce,
		RecordSeq: auth.RecordSeq,
	}
	return p, nil
}

func (c *Codec) decodeHandshakeMessage(fromAddr string, head *packetHeader, input []byte) (n *enode.Node, p Packet, err error) {
	node, nonce, session, err := c.decodeHandshake(fromAddr, head)
	if err != nil {
		return nil, nil, err
	}

	// Decrypt the message using the new session keys.
	msg, err := c.decryptMessage(input, nonce, session.readKey)
	if err != nil {
		return node, msg, err
	}

	// Handshake OK, drop the challenge and store the new session keys.
	c.sc.storeNewSession(head.SrcID, fromAddr, session)
	c.sc.deleteHandshake(head.SrcID, fromAddr)
	return node, msg, nil
}

func (c *Codec) decodeHandshake(fromAddr string, head *packetHeader) (*enode.Node, Nonce, *session, error) {
	auth, err := c.decodeHandshakeAuthData(head)
	if err != nil {
		return nil, Nonce{}, nil, err
	}

	// Verify against our last WHOAREYOU.
	challenge := c.sc.getHandshake(head.SrcID, fromAddr)
	if challenge == nil {
		return nil, Nonce{}, nil, errUnexpectedHandshake
	}
	// Get node record.
	node, err := c.decodeHandshakeRecord(challenge.Node, head.SrcID, auth.record)
	if err != nil {
		return nil, Nonce{}, nil, err
	}
	// Verify ephemeral key is on curve.
	ephkey, err := DecodePubkey(c.privkey.Curve, auth.pubkey)
	if err != nil {
		return nil, Nonce{}, nil, errInvalidAuthKey
	}
	// Verify ID nonce signature.
	err = verifyIDSignature(c.sha256, c.localnode.ID(), challenge.IDNonce[:], auth.pubkey, auth.signature, node)
	if err != nil {
		return nil, Nonce{}, nil, err
	}
	// Derive sesssion keys.
	session := c.deriveKeys(head.SrcID, c.localnode.ID(), c.privkey, ephkey, challenge)
	session = session.keysFlipped()
	return node, auth.h.Nonce, session, nil
}

// decodeHandshakeAuthData reads the authdata section of a handshake packet.
func (c *Codec) decodeHandshakeAuthData(head *packetHeader) (*handshakeAuthData, error) {
	// Decode fixed size part.
	if int(head.AuthSize) < sizeofHandshakeAuthDataV5 {
		return nil, fmt.Errorf("header authsize %d too low for handshake", head.AuthSize)
	}
	var auth handshakeAuthData
	binary.Read(&c.reader, binary.BigEndian, &auth.h)

	// Decode variable-size part.
	if c.reader.Len() < int(head.AuthSize) {
		return nil, errTooShort
	}
	varspace := int(head.AuthSize) - sizeofHandshakeAuthDataV5
	if int(auth.h.SigSize)+int(auth.h.PubkeySize) > varspace {
		return nil, fmt.Errorf("invalid handshake data sizes (%d+%d > %d)", auth.h.SigSize, auth.h.PubkeySize, varspace)
	}
	if !readNew(&auth.signature, int(auth.h.SigSize), &c.reader) {
		return nil, fmt.Errorf("can't read auth signature")
	}
	if !readNew(&auth.pubkey, int(auth.h.PubkeySize), &c.reader) {
		return nil, fmt.Errorf("can't read auth pubkey")
	}
	recordsize := varspace - int(auth.h.SigSize) - int(auth.h.PubkeySize)
	if !readNew(&auth.record, recordsize, &c.reader) {
		return nil, fmt.Errorf("can't read auth node record")
	}
	return &auth, nil
}

// readNew reads 'length' bytes from 'r' and stores them into 'data'.
func readNew(data *[]byte, length int, r *bytes.Reader) bool {
	if length == 0 {
		return true
	}
	*data = make([]byte, length)
	n, _ := r.Read(*data)
	return n == length
}

// decodeHandshakeRecord verifies the node record contained in a handshake packet. The
// remote node should include the record if we don't have one or if ours is older than the
// latest sequence number.
func (c *Codec) decodeHandshakeRecord(local *enode.Node, wantID enode.ID, remote []byte) (node *enode.Node, err error) {
	node = local
	if len(remote) > 0 {
		var record enr.Record
		if err := rlp.DecodeBytes(remote, &record); err != nil {
			return nil, err
		}
		if local == nil || local.Seq() < record.Seq() {
			n, err := enode.New(enode.ValidSchemes, &record)
			if err != nil {
				return nil, fmt.Errorf("invalid node record: %v", err)
			}
			if n.ID() != wantID {
				return nil, fmt.Errorf("record in handshake has wrong ID: %v", n.ID())
			}
			node = n
		}
	}
	if node == nil {
		err = errNoRecord
	}
	return node, err
}

// decodeMessage reads packet data following the header as an ordinary message packet.
func (c *Codec) decodeMessage(fromAddr string, head *packetHeader, input []byte) (Packet, error) {
	if c.reader.Len() < sizeofMessageAuthDataV5 {
		return nil, errTooShort
	}
	auth := new(messageAuthData)
	binary.Read(&c.reader, binary.BigEndian, auth)

	// Try decrypting the message.
	key := c.sc.readKey(head.SrcID, fromAddr)
	msg, err := c.decryptMessage(input, auth.Nonce, key)
	if err == errMessageDecrypt {
		// It didn't work. Start the handshake since this is an ordinary message packet.
		return &Unknown{AuthTag: auth.Nonce}, nil
	}
	return msg, err
}

func (c *Codec) decryptMessage(input []byte, nonce Nonce, readKey []byte) (Packet, error) {
	headerData := input[:len(input)-c.reader.Len()]
	messageCT := input[len(headerData):]
	message, err := decryptGCM(readKey, nonce[:], messageCT, headerData)
	if err != nil {
		return nil, errMessageDecrypt
	}
	if len(message) == 0 {
		return nil, errMessageTooShort
	}
	return DecodeMessage(message[0], message[1:])
}

// deriveKeys generates session keys using elliptic-curve Diffie-Hellman key agreement.
func (c *Codec) deriveKeys(n1, n2 enode.ID, priv *ecdsa.PrivateKey, pub *ecdsa.PublicKey, challenge *Whoareyou) *session {
	eph := ecdh(priv, pub)
	if eph == nil {
		return nil
	}

	info := []byte("discovery v5 key agreement")
	info = append(info, n1[:]...)
	info = append(info, n2[:]...)
	kdf := hkdf.New(c.sha256reset, eph, challenge.IDNonce[:], info)
	sec := session{
		writeKey: make([]byte, aesKeySize),
		readKey:  make([]byte, aesKeySize),
	}
	kdf.Read(sec.writeKey)
	kdf.Read(sec.readKey)
	for i := range eph {
		eph[i] = 0
	}
	return &sec
}

// sha256 returns the shared hash instance.
func (c *Codec) sha256reset() hash.Hash {
	c.sha256.Reset()
	return c.sha256
}

// maskOutputPacket applies protocol header masking to a packet sent to destID.
func (c *Codec) maskOutputPacket(destID enode.ID, output []byte, headerDataLen int) []byte {
	masked := make([]byte, sizeofMaskingIV+len(output))
	c.sc.maskingIVGen(masked[:sizeofMaskingIV])
	mask := headerMask(destID, masked)
	copy(masked[sizeofMaskingIV:], output)
	mask.XORKeyStream(masked[sizeofMaskingIV:], output[:headerDataLen])
	return masked
}

// headerMask returns a cipher for 'masking' / 'unmasking' packet headers.
func headerMask(destID enode.ID, input []byte) cipher.Stream {
	block, err := aes.NewCipher(destID[:16])
	if err != nil {
		panic("can't create cipher")
	}
	return cipher.NewCTR(block, input[:sizeofMaskingIV])
}
