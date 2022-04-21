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

package mclock

import (
	"time"
)

type Alarm struct {
	ch       chan struct{}
	clock    Clock
	timer    Timer
	deadline AbsTime
}

func NewAlarm(clock Clock) *Alarm {
	if clock == nil {
		panic("nil clock")
	}
	return &Alarm{
		ch:    make(chan struct{}, 1),
		clock: clock,
	}
}

func (e *Alarm) C() <-chan struct{} {
	return e.ch
}

func (e *Alarm) Schedule(time AbsTime) {
	now := e.clock.Now()
	e.schedule(now, time)
}

func (e *Alarm) Stop() {
	// Clear timer.
	if e.timer != nil {
		e.timer.Stop()
	}
	e.deadline = 0

	// Drain the channel.
	select {
	case <-e.ch:
	default:
	}
}

func (e *Alarm) schedule(now, newDeadline AbsTime) {
	if e.timer != nil {
		if e.deadline > now && e.deadline <= newDeadline {
			// Here, the current timer can be reused because it is already scheduled to
			// occur earlier than the new deadline.
			//
			// The e.deadline >= now part of the condition is important. If the old
			// deadline lies in the past, we assume the timer has already fired and needs
			// to be rescheduled.
			return
		}
		e.timer.Stop()
	}

	// Set the timer.
	d := time.Duration(0)
	if newDeadline < now {
		newDeadline = now
	} else {
		d = newDeadline.Sub(now)
	}
	e.timer = e.clock.AfterFunc(d, e.send)
	e.deadline = newDeadline
}

func (e *Alarm) send() {
	select {
	case e.ch <- struct{}{}:
	default:
	}
}
