// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/cmd/devp2p/internal/v5test"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

var (
	discv5Command = &cli.Command{
		Name:  "discv5",
		Usage: "Node Discovery v5 tools",
		Subcommands: []*cli.Command{
			discv5PingCommand,
			discv5ResolveCommand,
			discv5CrawlCommand,
			discv5TestCommand,
			discv5ListenCommand,
		},
	}
	discv5PingCommand = &cli.Command{
		Name:   "ping",
		Usage:  "Sends ping to a node",
		Action: discv5Ping,
	}
	discv5ResolveCommand = &cli.Command{
		Name:   "resolve",
		Usage:  "Finds a node in the DHT",
		Action: discv5Resolve,
		Flags:  []cli.Flag{bootnodesFlag},
	}
	discv5CrawlCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Updates a nodes.json file with random nodes found in the DHT",
		Action: discv5Crawl,
		Flags:  []cli.Flag{bootnodesFlag, crawlTimeoutFlag},
	}
	discv5TestCommand = &cli.Command{
		Name:   "test",
		Usage:  "Runs protocol tests against a node",
		Action: discv5Test,
		Flags: []cli.Flag{
			testPatternFlag,
			testTAPFlag,
			testListen1Flag,
			testListen2Flag,
		},
	}
	discv5ListenCommand = &cli.Command{
		Name:   "listen",
		Usage:  "Runs a node",
		Action: discv5Listen,
		Flags: []cli.Flag{
			bootnodesFlag,
			nodekeyFlag,
			nodedbFlag,
			nodeConfigFlag,
			listenAddrFlag,
			httpAddrFlag,
		},
	}
)

var (
	nodeConfigFlag = &cli.StringFlag{
		Name:  "config",
		Usage: "Path to JSON config file",
	}
)

func discv5Ping(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc := startV5(ctx, nil)
	defer disc.Close()

	fmt.Println(disc.Ping(n))
	return nil
}

func discv5Resolve(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc := startV5(ctx, nil)
	defer disc.Close()

	fmt.Println(disc.Resolve(n))
	return nil
}

func discv5Crawl(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return fmt.Errorf("need nodes file as argument")
	}
	nodesFile := ctx.Args().First()
	var inputSet nodeSet
	if common.FileExist(nodesFile) {
		inputSet = loadNodesJSON(nodesFile)
	}

	disc := startV5(ctx, nil)
	defer disc.Close()
	c := newCrawler(inputSet, disc, disc.RandomNodes())
	c.revalidateInterval = 10 * time.Minute
	output := c.run(ctx.Duration(crawlTimeoutFlag.Name))
	writeNodesJSON(nodesFile, output)
	return nil
}

// discv5Test runs the protocol test suite.
func discv5Test(ctx *cli.Context) error {
	suite := &v5test.Suite{
		Dest:    getNodeArg(ctx),
		Listen1: ctx.String(testListen1Flag.Name),
		Listen2: ctx.String(testListen2Flag.Name),
	}
	return runTests(ctx, suite.AllTests())
}

func discv5Listen(ctx *cli.Context) error {
	var extConfig *discv5NodeConfig
	if configFile := ctx.String(nodeConfigFlag.Name); configFile != "" {
		cfg, err := loadConfig(configFile)
		if err != nil {
			return fmt.Errorf("can't load config file: %v", err)
		}
		extConfig = cfg
	}

	disc := startV5(ctx, extConfig)
	defer disc.Close()

	fmt.Println(disc.Self())

	httpAddr := ctx.String(httpAddrFlag.Name)
	if httpAddr == "" {
		// Non-HTTP mode.
		select {}
		return nil
	}

	api := &discAPI{
		host:          disc,
		searchTimeout: defaultSearchTimeout,
	}
	if extConfig != nil && extConfig.SearchTimeoutSeconds > 0 {
		api.searchTimeout = time.Duration(extConfig.SearchTimeoutSeconds) * time.Second
	}
	log.Info("Starting RPC API server", "addr", httpAddr)
	srv := rpc.NewServer()
	srv.RegisterName("discv5", api)
	httpsrv := http.Server{Addr: httpAddr, Handler: srv}
	return httpsrv.ListenAndServe()
}

// startV5 starts an ephemeral discovery v5 node.
func startV5(ctx *cli.Context, extConfig *discv5NodeConfig) *discover.UDPv5 {
	ln, config := makeDiscoveryConfig(ctx)
	if extConfig != nil {
		config.Topic.AdCacheSize = extConfig.AdCacheSize
		config.Topic.AdLifetime = time.Duration(extConfig.AdLifetimeSeconds) * time.Second
		config.Topic.RegBucketSize = extConfig.RegBucketSize
		config.Topic.RegAttemptTimeout = time.Duration(extConfig.RegTimeoutSeconds) * time.Second
		config.Topic.SearchBucketSize = extConfig.SearchBucketSize
	}

	socket := listen(ln, ctx.String(listenAddrFlag.Name))
	disc, err := discover.ListenV5(socket, ln, config)
	if err != nil {
		exit(err)
	}
	return disc
}

// ---------------------------
// JSON tester

const defaultSearchTimeout = 120 * time.Second

type discv5NodeConfig struct {
	AdCacheSize       int `json:"adCacheSize"`
	AdLifetimeSeconds int `json:"adLifetimeSeconds"`
	RegBucketSize     int `json:"regBucketSize"`
	RegTimeoutSeconds int `json:"regTimeoutSeconds"`
	SearchBucketSize  int `json:"searchBucketSize"`

	SearchTimeoutSeconds int `json:"searchTimeoutSeconds"`
}

func loadConfig(file string) (*discv5NodeConfig, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	var cfg discv5NodeConfig
	dec := json.NewDecoder(fd)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

type discAPI struct {
	host          *discover.UDPv5
	searchTimeout time.Duration
}

func (api *discAPI) RegisterTopic(topic common.Hash, opID *uint64) {
	var op uint64
	if opID != nil {
		op = *opID
	}
	api.host.RegisterTopic(topicindex.TopicID(topic), op)
}

func (api *discAPI) UnregisterTopic(topic common.Hash) {
	api.host.StopRegisterTopic(topicindex.TopicID(topic))
}

func (api *discAPI) NodeTable() []*enode.Node {
	return api.host.AllNodes()
}

func (api *discAPI) TopicNodes(topic common.Hash) []*enode.Node {
	return api.host.LocalTopicNodes(topicindex.TopicID(topic))
}

func (api *discAPI) TopicSearch(topic common.Hash, numNodes int, opID *uint64) []enode.ID {
	var op uint64
	if opID != nil {
		op = *opID
	}

	it := api.host.TopicSearch(topicindex.TopicID(topic), op)
	defer it.Close()

	var (
		nodes      = make(chan []*enode.Node, 1)
		searchDone = make(chan struct{})
		wg         sync.WaitGroup
	)

	// This closes the iterator when search is done or times out.
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeout := time.NewTimer(api.searchTimeout)
		defer timeout.Stop()
		select {
		case <-timeout.C:
		case <-searchDone:
		}
		it.Close()
	}()

	// This reads nodes from the iterator.
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodes <- enode.ReadNodes(it, numNodes)
		close(searchDone)
	}()

	wg.Wait()
	ids := make([]enode.ID, len(nodes))
	for i, n := range <-nodes {
		ids[i] = n.ID()
	}
	return ids
}

func (api *discAPI) LocalNode() *enode.Node {
	return api.host.Self()
}
