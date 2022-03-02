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
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// This test checks basic ticket encoding/decoding.
func TestTicketSealerSimple(t *testing.T) {
	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	ticket := &Ticket{
		Topic:          topic1,
		WaitTimeTotal:  5 * time.Minute,
		WaitTimeIssued: 1 * time.Minute,
		LastUsed:       clock.Now(),
	}
	enc := ts.Pack(ticket)
	if len(enc) != encTicketFullSize {
		t.Fatal("encoded ticket has wrong size")
	}

	clock.Run(1 * time.Minute)

	ticket2, err := ts.Unpack(ticket.Topic, enc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ticket, ticket2) {
		t.Fatal("decoded ticket not equal")
	}
}

func TestTicketSealerInvalid(t *testing.T) {
	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	enc := ts.Pack(&Ticket{
		Topic:          topic1,
		WaitTimeTotal:  5 * time.Minute,
		WaitTimeIssued: 5 * time.Minute,
		LastUsed:       clock.Now(),
	})
	if _, err := ts.Unpack(topic2, enc); err != errWrongTicketTopic {
		t.Fatal("unpacking with wrong topic did not raise correct error")
	}
	if _, err := ts.Unpack(topic1, enc); err != errTicketTimeInvalid {
		t.Fatal("unpacking at wrong time did not raise correct error")
	}
}

// This test checks that tickets sealed with an older key
// are accepted after a key rotation happened.
func TestTicketSealerKeyRotation(t *testing.T) {
	prevRekeyInterval := ticketRekeyInterval
	defer func() { ticketRekeyInterval = prevRekeyInterval }()

	ticketRekeyInterval = 10

	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	var encTickets [][]byte
	for i := 0; i < ticketRekeyInterval*2; i++ {
		ticket := &Ticket{
			Topic:          topic1,
			WaitTimeTotal:  5 * time.Minute,
			WaitTimeIssued: ticketKeyLifetime - 3,
			LastUsed:       clock.Now(),
		}
		encTickets = append(encTickets, ts.Pack(ticket))
	}

	clock.Run(ticketKeyLifetime - 1)
	for i, enc := range encTickets {
		if _, err := ts.Unpack(topic1, enc); err != nil {
			t.Fatalf("can't unpack ticket %d: %v", i, err)
		}
	}

	clock.Run(1)
	for i, enc := range encTickets {
		if _, err := ts.Unpack(topic1, enc); err != errNoTicketKey {
			t.Fatalf("Unpack returned wrong error for ticket %d with expired key: %v", i, err)
		}
	}
}
