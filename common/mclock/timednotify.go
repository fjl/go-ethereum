// Copyright 2018 The go-ethereum Authors
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
	"sync"
	"time"
)

type TimedNotify struct {
	ch    chan struct{}
	clock Clock

	mu       sync.Mutex
	timer    Timer
	deadline AbsTime
}

func NewEvent(clock Clock) *TimedNotify {
	if clock == nil {
		panic("nil clock")
	}
	return &TimedNotify{
		ch:    make(chan struct{}, 1),
		clock: clock,
	}
}

func (e *TimedNotify) Ch() <-chan struct{} {
	return e.ch
}

func (e *TimedNotify) Schedule(d time.Duration) {
	now := e.clock.Now()
	e.schedule(now, now.Add(d))
}

func (e *TimedNotify) ScheduleAt(time AbsTime) {
	now := e.clock.Now()
	e.schedule(now, time)
}

func (e *TimedNotify) schedule(now, newDeadline AbsTime) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Re-use current timer if it would fire earlier than the new deadline.
	if e.timer != nil {
		switch {
		case e.deadline <= now:
			// This case is special: the timer has already expired.
		case e.deadline <= newDeadline:
			return
		}
		e.timer.Stop()
		e.timer = nil
	}

	// Set the timer.
	d := time.Duration(0)
	if newDeadline > now {
		d = newDeadline.Sub(now)
		newDeadline = now
	}
	e.timer = e.clock.AfterFunc(d, e.send)
	e.deadline = newDeadline
}

func (e *TimedNotify) send() {
	select {
	case e.ch <- struct{}{}:
	default:
	}
}
