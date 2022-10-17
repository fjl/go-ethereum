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
	"encoding/hex"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Config is the configuration of the topic system.
type Config struct {
	Self enode.ID // the node's own ID

	// Topic table settings.
	AdLifetime  time.Duration
	AdCacheSize int

	// Registration settings.
	RegBucketSize         int           // max/ number of active nodes in registration bucket
	RegBucketStandbyLimit int           // max. number of 'standby' state nodes in bucket
	RegAttemptTimeout     time.Duration // maximum amount of time to wait on one attempt

	// Search settings.
	SearchBucketSize int // number of nodes in search buckets

	// These settings are exposed for testing purposes.
	Clock mclock.Clock
	Log   log.Logger
}

// withDefaults configures defaults for unset config options.
func (cfg Config) withDefaults() Config {
	if cfg.AdLifetime == 0 {
		cfg.AdLifetime = 15 * time.Minute
	}
	if cfg.AdCacheSize == 0 {
		cfg.AdCacheSize = 5000
	}
	if cfg.RegAttemptTimeout == 0 {
		// Note: RegAttemptTimeout == AdLifetime is a good choice because, when AdLifetime
		// has passed, all ads will have cycled in the remote table. If registration still
		// hasn't worked after this time, the registrar is overloaded or malfunctioning
		// and it's better to pick another one.
		cfg.RegAttemptTimeout = cfg.AdLifetime + cfg.AdLifetime/2
	}
	if cfg.RegBucketSize == 0 {
		cfg.RegBucketSize = 10
	}
	if cfg.RegBucketStandbyLimit == 0 {
		cfg.RegBucketStandbyLimit = 20
	}
	if cfg.SearchBucketSize == 0 {
		cfg.SearchBucketSize = 8
	}

	if cfg.Log == nil {
		cfg.Log = log.Root()
	}
	if cfg.Clock == nil {
		cfg.Clock = mclock.System{}
	}
	return cfg
}

// TopicID represents a topic.
type TopicID [32]byte

func (t TopicID) TerminalString() string {
	return hex.EncodeToString(t[:8])
}

func (t TopicID) String() string {
	return hex.EncodeToString(t[:])
}

// Never is a special time value returned by certain event-scheduling functions.
// It indicates that the event should not be scheduled.
const Never = ^mclock.AbsTime(0)
