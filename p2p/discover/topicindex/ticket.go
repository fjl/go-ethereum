// Copyright 2022 The go-ethereum Authors
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

package topicindex

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// This is the maximum number of times a ticketKey can be used for encoding.
// When reached, TicketSealer creates a new key.
var ticketRekeyInterval = 100000

const (
	ticketKeyLifetime    = 6 * time.Hour
	ticketValidityWindow = 5 * time.Second
	ticketMacSize        = 32
)

// Ticket defines the content of generated tickets.
type Ticket struct {
	KeyID          uint16
	Topic          TopicID
	WaitTimeIssued time.Duration
	LastUsed       mclock.AbsTime
	FirstIssued    mclock.AbsTime
}

func (t Ticket) isValidAt(now mclock.AbsTime) bool {
	start := t.LastUsed.Add(t.WaitTimeIssued)
	end := start.Add(ticketValidityWindow)
	return now >= start && now < end
}

// TicketSealer can encode/decode tickets.
type TicketSealer struct {
	keys      []*ticketKey
	clock     mclock.Clock
	idCounter uint16
}

var (
	encTicketSize     = binary.Size(Ticket{})
	encTicketFullSize = ticketMacSize + encTicketSize
)

// ticketKey is a MAC key held by TicketSealer.
type ticketKey struct {
	keyID     uint16
	hmac      hash.Hash
	macBuffer []byte

	uses     int
	lastUsed mclock.AbsTime
}

func NewTicketSealer(clock mclock.Clock) *TicketSealer {
	return &TicketSealer{clock: clock}
}

var (
	errBadTicketSize     = errors.New("encoded ticket has wrong size")
	errBadTicketMAC      = errors.New("invalid ticket MAC")
	errNoTicketKey       = errors.New("unknown ticket MAC keyID")
	errWrongTicketTopic  = errors.New("ticket used with wrong topic")
	errTicketTimeInvalid = errors.New("ticket used at the wrong time")
)

// Unpack decrypts, authenticates and deserializes a ticket.
func (ts *TicketSealer) Unpack(topic TopicID, input []byte) (*Ticket, error) {
	// Empty input means the sender has no ticket.
	if len(input) == 0 {
		return &Ticket{Topic: topic}, nil
	}
	// If non-empty, the ticket size must match exactly.
	if len(input) != encTicketFullSize {
		return nil, errBadTicketSize
	}

	// Read the ticket data.
	// Not great that this happens before checking MAC,
	// but KeyID from Ticket is needed to find the right key.
	enc := input[ticketMacSize:]
	var ticket Ticket
	r := bytes.NewReader(enc)
	if err := binary.Read(r, binary.BigEndian, &ticket); err != nil {
		return nil, err
	}

	// Find the matching MAC key.
	ts.gcKeys()
	mac := input[:ticketMacSize]
	for _, key := range ts.keys {
		if key.keyID != ticket.KeyID {
			continue
		}
		computed := key.computeMAC(enc)
		if !hmac.Equal(mac, computed) {
			return nil, errBadTicketMAC
		}
		if ticket.Topic != topic {
			return nil, errWrongTicketTopic
		}
		if !ticket.isValidAt(ts.clock.Now()) {
			return nil, errTicketTimeInvalid
		}
		return &ticket, nil
	}
	return nil, errNoTicketKey
}

// Pack encodes a ticket and seals it.
// Note: this modifies KeyID of ticket.
func (ts *TicketSealer) Pack(ticket *Ticket) []byte {
	ts.gcKeys()
	key := ts.getValidKey()
	ticket.KeyID = key.keyID

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, ticket); err != nil {
		panic(err)
	}

	mac := key.computeMAC(buf.Bytes())
	output := make([]byte, len(mac)+buf.Len())
	copy(output, mac)
	copy(output[len(mac):], buf.Bytes())
	return output
}

// gcKeys removes old keys.
func (ts *TicketSealer) gcKeys() {
	deadline := ts.clock.Now().Add(-ticketKeyLifetime)
	cut := 0
	for _, key := range ts.keys {
		if key.lastUsed > deadline {
			break
		}
		cut++
	}
	if cut > 0 {
		copy(ts.keys, ts.keys[cut:])
		ts.keys = ts.keys[:len(ts.keys)-cut]
	}
}

// getValidKey ensures there is at least one valid key and returns it.
func (ts *TicketSealer) getValidKey() *ticketKey {
	if len(ts.keys) > 0 && ts.keys[len(ts.keys)-1].uses < ticketRekeyInterval {
		// Current latest key is still valid.
		key := ts.keys[len(ts.keys)-1]
		key.uses++
		key.lastUsed = ts.clock.Now()
	}

	// Generate key ID.
	id := ts.idCounter
	ts.idCounter++

	// Generate key.
	keybytes := make([]byte, 32)
	if _, err := rand.Read(keybytes); err != nil {
		panic("can't get random data")
	}
	mac := hmac.New(sha256.New, keybytes)
	key := &ticketKey{
		hmac:     mac,
		keyID:    id,
		uses:     1,
		lastUsed: ts.clock.Now(),
	}
	if ticketMacSize != key.hmac.Size() {
		panic("ticketMacSize is wrong: " + strconv.Itoa(key.hmac.Size()))
	}

	// Store the key.
	ts.keys = append(ts.keys, key)
	return key
}

func (key *ticketKey) computeMAC(data []byte) []byte {
	key.hmac.Reset()
	key.hmac.Write(data)
	key.macBuffer = key.hmac.Sum(key.macBuffer[:0])
	return key.macBuffer
}
