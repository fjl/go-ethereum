// Copyright 2019 The go-ethereum Authors
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
	"crypto/ecdsa"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/cmd/devp2p/internal/v4test"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/v4wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/urfave/cli.v1"
)

var (
	discv4Command = cli.Command{
		Name:  "discv4",
		Usage: "Node Discovery v4 tools",
		Subcommands: []cli.Command{
			discv4PingCommand,
			discv4PingIPCommand,
			discv4RequestRecordCommand,
			discv4ResolveCommand,
			discv4ResolveJSONCommand,
			discv4CrawlCommand,
			discv4TestCommand,
		},
	}
	discv4PingCommand = cli.Command{
		Name:      "ping",
		Usage:     "Sends ping to a node",
		Action:    discv4Ping,
		ArgsUsage: "<node>",
		Flags:     []cli.Flag{nodekeyFlag, nodekeySeedFlag, listenAddrFlag, bootnodesFlag},
	}
	discv4PingIPCommand = cli.Command{
		Name:      "pingip",
		Usage:     "Sends ping to IPs",
		Action:    discv4PingIP,
		ArgsUsage: "<ip:port>...",
		Flags:     []cli.Flag{nodekeyFlag, nodekeySeedFlag, listenAddrFlag},
	}
	discv4RequestRecordCommand = cli.Command{
		Name:      "requestenr",
		Usage:     "Requests a node record using EIP-868 enrRequest",
		Action:    discv4RequestRecord,
		ArgsUsage: "<node>",
		Flags:     []cli.Flag{nodekeyFlag, nodekeySeedFlag, listenAddrFlag},
	}
	discv4ResolveCommand = cli.Command{
		Name:      "resolve",
		Usage:     "Finds a node in the DHT",
		Action:    discv4Resolve,
		ArgsUsage: "<node>",
		Flags:     []cli.Flag{nodekeyFlag, nodekeySeedFlag, bootnodesFlag, listenAddrFlag},
	}
	discv4ResolveJSONCommand = cli.Command{
		Name:      "resolve-json",
		Usage:     "Re-resolves nodes in a nodes.json file",
		Action:    discv4ResolveJSON,
		Flags:     []cli.Flag{bootnodesFlag},
		ArgsUsage: "<nodes.json file>",
	}
	discv4CrawlCommand = cli.Command{
		Name:   "crawl",
		Usage:  "Updates a nodes.json file with random nodes found in the DHT",
		Action: discv4Crawl,
		Flags:  []cli.Flag{bootnodesFlag, crawlTimeoutFlag},
	}
	discv4TestCommand = cli.Command{
		Name:   "test",
		Usage:  "Runs tests against a node",
		Action: discv4Test,
		Flags: []cli.Flag{
			remoteEnodeFlag,
			testPatternFlag,
			testTAPFlag,
			testListen1Flag,
			testListen2Flag,
		},
	}
)

var (
	bootnodesFlag = cli.StringFlag{
		Name:  "bootnodes",
		Usage: "Comma separated nodes used for bootstrapping",
	}
	nodekeyFlag = cli.StringFlag{
		Name:  "nodekey",
		Usage: "Hex-encoded node key",
	}
	nodekeySeedFlag = cli.StringFlag{
		Name:  "seed",
		Usage: "Sets deterministic nodekey",
	}
	nodedbFlag = cli.StringFlag{
		Name:  "nodedb",
		Usage: "Nodes database location",
	}
	listenAddrFlag = cli.StringFlag{
		Name:  "addr",
		Usage: "Listening address",
		Value: ":0",
	}
	crawlTimeoutFlag = cli.DurationFlag{
		Name:  "timeout",
		Usage: "Time limit for the crawl.",
		Value: 30 * time.Minute,
	}
	remoteEnodeFlag = cli.StringFlag{
		Name:   "remote",
		Usage:  "Enode of the remote node under test",
		EnvVar: "REMOTE_ENODE",
	}
)

func discv4Ping(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc := startV4(ctx)
	defer disc.Close()

	start := time.Now()
	if err := disc.Ping(n); err != nil {
		return fmt.Errorf("node didn't respond: %v", err)
	}
	fmt.Printf("node responded to ping (RTT %v).\n", time.Since(start))
	return nil
}

func discv4PingIP(ctx *cli.Context) error {
	var addrs []*net.UDPAddr
	for _, as := range ctx.Args() {
		addr, err := net.ResolveUDPAddr("udp", as)
		if err != nil {
			fmt.Println("skipping invalid addr", as)
		}
		addrs = append(addrs, addr)
	}

	for _, addr := range addrs {
		fmt.Printf("%v: sending ping\n", addr)
		err := rawPing(ctx, addr)
		if err != nil {
			fmt.Printf("%v: %v\n", addr, err)
		}
	}
	return nil
}

func rawPing(ctx *cli.Context, addr *net.UDPAddr) error {
	laddr := ctx.String(listenAddrFlag.Name)
	socket, err := net.ListenPacket("udp", laddr)
	if err != nil {
		panic(err)
	}
	defer socket.Close()

	privkey := makeNodekey(ctx)

	ping := &v4wire.Ping{
		Version:    4,
		To:         v4wire.NewEndpoint(addr, 0),
		Expiration: uint64(time.Now().Unix()) + 20,
	}
	packet, _, err := v4wire.Encode(privkey, ping)
	if err != nil {
		panic(err)
	}
	if _, err = socket.WriteTo(packet, addr); err != nil {
		return err
	}

	var buffer = make([]byte, 1280)
	socket.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		buffer = buffer[:cap(buffer)]
		n, fromAddr, err := socket.ReadFrom(buffer)
		if err != nil {
			return err
		}
		buffer = buffer[:n]
		if fromAddr.String() == addr.String() {
			break
		} else {
			fmt.Printf("got response from different IP %v", fromAddr)
		}
	}
	resp, pubkey, _, err := v4wire.Decode(buffer)
	if err != nil {
		return err
	}
	fmt.Printf("%v: is enode://%x@%v\n", addr, pubkey[:], addr)
	fmt.Printf("%v: %s %+v\n", addr, resp.Name(), resp)
	return nil
}

func discv4RequestRecord(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc := startV4(ctx)
	defer disc.Close()

	respN, err := disc.RequestENR(n)
	if err != nil {
		return fmt.Errorf("can't retrieve record: %v", err)
	}
	fmt.Println(respN.String())
	return nil
}

func discv4Resolve(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc := startV4(ctx)
	defer disc.Close()

	fmt.Println(disc.Resolve(n).String())
	return nil
}

func discv4ResolveJSON(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return fmt.Errorf("need nodes file as argument")
	}
	nodesFile := ctx.Args().Get(0)
	inputSet := make(nodeSet)
	if common.FileExist(nodesFile) {
		inputSet = loadNodesJSON(nodesFile)
	}

	// Add extra nodes from command line arguments.
	var nodeargs []*enode.Node
	for i := 1; i < ctx.NArg(); i++ {
		n, err := parseNode(ctx.Args().Get(i))
		if err != nil {
			exit(err)
		}
		nodeargs = append(nodeargs, n)
	}

	// Run the crawler.
	disc := startV4(ctx)
	defer disc.Close()
	c := newCrawler(inputSet, disc, enode.IterNodes(nodeargs))
	c.revalidateInterval = 0
	output := c.run(0)
	writeNodesJSON(nodesFile, output)
	return nil
}

func discv4Crawl(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return fmt.Errorf("need nodes file as argument")
	}
	nodesFile := ctx.Args().First()
	var inputSet nodeSet
	if common.FileExist(nodesFile) {
		inputSet = loadNodesJSON(nodesFile)
	}

	disc := startV4(ctx)
	defer disc.Close()
	c := newCrawler(inputSet, disc, disc.RandomNodes())
	c.revalidateInterval = 10 * time.Minute
	output := c.run(ctx.Duration(crawlTimeoutFlag.Name))
	writeNodesJSON(nodesFile, output)
	return nil
}

// discv4Test runs the protocol test suite.
func discv4Test(ctx *cli.Context) error {
	// Configure test package globals.
	if !ctx.IsSet(remoteEnodeFlag.Name) {
		return fmt.Errorf("Missing -%v", remoteEnodeFlag.Name)
	}
	v4test.Remote = ctx.String(remoteEnodeFlag.Name)
	v4test.Listen1 = ctx.String(testListen1Flag.Name)
	v4test.Listen2 = ctx.String(testListen2Flag.Name)
	return runTests(ctx, v4test.AllTests)
}

// startV4 starts an ephemeral discovery V4 node.
func startV4(ctx *cli.Context) *discover.UDPv4 {
	ln, config := makeDiscoveryConfig(ctx)
	socket := listen(ln, ctx.String(listenAddrFlag.Name))
	disc, err := discover.ListenV4(socket, ln, config)
	if err != nil {
		exit(err)
	}
	return disc
}

func makeNodekey(ctx *cli.Context) *ecdsa.PrivateKey {
	switch {
	case ctx.IsSet(nodekeyFlag.Name):
		key, err := crypto.HexToECDSA(ctx.String(nodekeyFlag.Name))
		if err != nil {
			exit(fmt.Errorf("-%s: %v", nodekeyFlag.Name, err))
		}
		return key
	case ctx.IsSet(nodekeySeedFlag.Name):
		keydata := crypto.Keccak256([]byte(ctx.String(nodekeySeedFlag.Name)))
		key, err := crypto.ToECDSA(keydata)
		if err != nil {
			exit(fmt.Errorf("-%s: %v", nodekeySeedFlag.Name, err))
		}
		return key
	default:
		key, err := crypto.GenerateKey()
		if err != nil {
			exit(err)
		}
		return key
	}
}

func makeDiscoveryConfig(ctx *cli.Context) (*enode.LocalNode, discover.Config) {
	var cfg discover.Config
	cfg.PrivateKey = makeNodekey(ctx)

	if commandHasFlag(ctx, bootnodesFlag) {
		bn, err := parseBootnodes(ctx)
		if err != nil {
			exit(err)
		}
		cfg.Bootnodes = bn
	}

	dbpath := ctx.String(nodedbFlag.Name)
	db, err := enode.OpenDB(dbpath)
	if err != nil {
		exit(err)
	}
	ln := enode.NewLocalNode(db, cfg.PrivateKey)
	return ln, cfg
}

func listen(ln *enode.LocalNode, addr string) *net.UDPConn {
	if addr == "" {
		addr = "0.0.0.0:0"
	}
	socket, err := net.ListenPacket("udp4", addr)
	if err != nil {
		exit(err)
	}
	usocket := socket.(*net.UDPConn)
	uaddr := socket.LocalAddr().(*net.UDPAddr)
	if uaddr.IP.IsUnspecified() {
		ln.SetFallbackIP(net.IP{127, 0, 0, 1})
	} else {
		ln.SetFallbackIP(uaddr.IP)
	}
	ln.SetFallbackUDP(uaddr.Port)
	return usocket
}

func parseBootnodes(ctx *cli.Context) ([]*enode.Node, error) {
	s := params.RinkebyBootnodes
	if ctx.IsSet(bootnodesFlag.Name) {
		input := ctx.String(bootnodesFlag.Name)
		if input == "" {
			return nil, nil
		}
		s = strings.Split(input, ",")
	}
	nodes := make([]*enode.Node, len(s))
	var err error
	for i, record := range s {
		nodes[i], err = parseNode(record)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap node: %v", err)
		}
	}
	return nodes, nil
}
