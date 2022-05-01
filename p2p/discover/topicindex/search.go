package topicindex

import (
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Search is the state associated with searching for a single topic.
type Search struct {
	clock   mclock.Clock
	log     log.Logger
	topic   TopicID
	buckets [40]searchBucket
}

type searchBucket struct{}

func NewSearch(topic TopicID, config Config) *Search {
	config = config.withDefaults()
	s := &Search{
		clock: config.Clock,
		log:   config.Log,
	}
	return s
}

func (s *Search) Topic() TopicID {
	return s.topic
}

func (s *Search) NextLookupTime() mclock.AbsTime {
	return 0
}

func (s *Search) LookupTarget() enode.ID {
	return enode.ID{}
}

func (s *Search) AddNodes(nodes []*enode.Node) {
	for _, n := range nodes {
		s.add(n)
	}
}

func (s *Search) add(n *enode.Node) {
}

func (s *Search) QueryTarget() *enode.Node {
	return nil
}
