// Copyright 2020 The go-ethereum Authors
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

package eth

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
)

var (
	testBalance = big.NewInt(2e10)
)

func generateTestChain() (*core.Genesis, []*types.Block) {
	db := rawdb.NewMemoryDatabase()
	config := params.AllEthashProtocolChanges
	genesis := &core.Genesis{
		Config:    config,
		Alloc:     core.GenesisAlloc{testAddr: {Balance: testBalance}},
		ExtraData: []byte("test genesis"),
		Timestamp: 9000,
	}
	generate := func(i int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("test"))
	}
	gblock := genesis.ToBlock(db)
	engine := ethash.NewFaker()
	blocks, _ := core.GenerateChain(config, gblock, engine, db, 10, generate)
	blocks = append([]*types.Block{gblock}, blocks...)
	return genesis, blocks
}

func generateTestChainWithFork(n int, fork int) (*core.Genesis, []*types.Block, []*types.Block) {
	if fork >= n {
		fork = n - 1
	}
	db := rawdb.NewMemoryDatabase()
	config := &params.ChainConfig{big.NewInt(1337), big.NewInt(0), nil, false, big.NewInt(0), common.Hash{}, big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), nil, nil, big.NewInt(0), new(params.EthashConfig), nil}
	genesis := &core.Genesis{
		Config:    config,
		Alloc:     core.GenesisAlloc{testAddr: {Balance: testBalance}},
		ExtraData: []byte("test genesis"),
		Timestamp: 9000,
	}
	generate := func(i int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("test"))
	}
	generateFork := func(i int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("testF"))
	}
	gblock := genesis.ToBlock(db)
	engine := ethash.NewFaker()
	blocks, _ := core.GenerateChain(config, gblock, engine, db, n, generate)
	blocks = append([]*types.Block{gblock}, blocks...)
	forkedBlocks, _ := core.GenerateChain(config, blocks[fork], engine, db, n-fork, generateFork)
	return genesis, blocks, forkedBlocks
}

func TestEth2AssembleBlock(t *testing.T) {
	genesis, blocks := generateTestChain()

	n, err := node.New(&node.Config{})
	if err != nil {
		t.Fatalf("could not get node: %v", err)
	}
	ethservice, err := New(n, &Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}})
	if err != nil {
		t.Fatalf("can't create new ethereum service: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("can't start test node: %v", err)
	}
	if _, err := ethservice.BlockChain().InsertChain(blocks[1:9]); err != nil {
		t.Fatalf("can't import test blocks: %v", err)
	}
	ethservice.SetEtherbase(testAddr)

	api := NewEth2API(ethservice)
	signer := types.NewEIP155Signer(ethservice.BlockChain().Config().ChainID)
	tx, err := types.SignTx(types.NewTransaction(0, blocks[8].Coinbase(), big.NewInt(1000), params.TxGas, nil, nil), signer, testKey)
	if err != nil {
		t.Fatalf("error signing transaction, err=%v", err)
	}
	ethservice.txPool.AddLocal(tx)
	blockParams := AssembleBlockParams{
		ParentHash: blocks[8].ParentHash(),
		Timestamp:  blocks[8].Time(),
	}
	execData, err := api.AssembleBlock(blockParams)

	if err != nil {
		t.Fatalf("error producing block, err=%v", err)
	}

	if len(execData.Transactions) != 1 {
		t.Fatalf("invalid number of transactions %d != 1", len(execData.Transactions))
	}
}

func TestEth2AssembleBlockWithAnotherBlocksTxs(t *testing.T) {
	genesis, blocks := generateTestChain()

	n, err := node.New(&node.Config{})
	if err != nil {
		t.Fatalf("could not get node: %v", err)
	}
	ethservice, err := New(n, &Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}})
	if err != nil {
		t.Fatalf("can't create new ethereum service: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("can't start test node: %v", err)
	}
	if _, err := ethservice.BlockChain().InsertChain(blocks[1:9]); err != nil {
		t.Fatalf("can't import test blocks: %v", err)
	}
	ethservice.SetEtherbase(testAddr)

	api := NewEth2API(ethservice)

	// Put the 10th block's tx in the pool and produce a new block
	api.addBlockTxs(blocks[9])
	blockParams := AssembleBlockParams{
		ParentHash: blocks[9].ParentHash(),
		Timestamp:  blocks[9].Time(),
	}
	execData, err := api.AssembleBlock(blockParams)
	if err != nil {
		t.Fatalf("error producing block, err=%v", err)
	}

	if len(execData.Transactions) != blocks[9].Transactions().Len() {
		t.Fatalf("invalid number of transactions %d != 1", len(execData.Transactions))
	}
}

func TestEth2NewBlock(t *testing.T) {
	genesis, blocks, forkedBlocks := generateTestChainWithFork(10, 4)

	n, err := node.New(&node.Config{})
	if err != nil {
		t.Fatalf("could not get node: %v", err)
	}
	ethservice, err := New(n, &Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}})
	if err != nil {
		t.Fatalf("can't create new ethereum service: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("can't start test node: %v", err)
	}
	if _, err := ethservice.BlockChain().InsertChain(blocks[1:5]); err != nil {
		t.Fatalf("can't import test blocks: %v", err)
	}

	api := NewEth2API(ethservice)
	for i := 5; i < 10; i++ {
		p := ExecutableData{
			ParentHash:   ethservice.BlockChain().CurrentBlock().Hash(),
			Miner:        blocks[i].Coinbase(),
			StateRoot:    blocks[i].Root(),
			GasLimit:     blocks[i].GasLimit(),
			GasUsed:      blocks[i].GasUsed(),
			Transactions: []*types.Transaction(blocks[i].Transactions()),
			ReceiptRoot:  blocks[i].ReceiptHash(),
			LogsBloom:    blocks[i].Bloom().Bytes(),
			BlockHash:    blocks[i].Hash(),
			Timestamp:    blocks[i].Time(),
		}
		success, err := api.NewBlock(p)
		if err != nil || !success.Valid {
			t.Fatalf("Failed to insert block: %v", err)
		}
	}

	exp := ethservice.BlockChain().CurrentBlock().Hash()

	// Introduce the fork point
	lastBlockNum := blocks[4].Number()
	lastBlock := blocks[4]
	for i := 0; i < 4; i++ {
		lastBlockNum.Add(lastBlockNum, big.NewInt(1))
		p := ExecutableData{
			ParentHash:   lastBlock.Hash(),
			Miner:        forkedBlocks[i].Coinbase(),
			StateRoot:    forkedBlocks[i].Root(),
			Number:       lastBlockNum.Uint64(),
			GasLimit:     forkedBlocks[i].GasLimit(),
			GasUsed:      forkedBlocks[i].GasUsed(),
			Transactions: []*types.Transaction(blocks[i].Transactions()),
			ReceiptRoot:  forkedBlocks[i].ReceiptHash(),
			LogsBloom:    forkedBlocks[i].Bloom().Bytes(),
			BlockHash:    forkedBlocks[i].Hash(),
			Timestamp:    forkedBlocks[i].Time(),
		}
		success, err := api.NewBlock(p)
		if err != nil || !success.Valid {
			t.Fatalf("Failed to insert forked block #%d: %v", i, err)
		}
		lastBlock = insertBlockParamsToBlock(p, lastBlockNum)
	}

	if ethservice.BlockChain().CurrentBlock().Hash() != exp {
		t.Fatalf("Wrong head after inserting fork %x != %x", exp, ethservice.BlockChain().CurrentBlock().Hash())
	}
}

//func TestEth2SetHead(t *testing.T) {
//genesis, blocks, forkedBlocks := generateTestChainWithFork(10, 5)

//n, err := node.New(&node.Config{})
//if err != nil {
//t.Fatalf("could not get node: %v", err)
//}
//ethservice, err := New(n, &Config{Genesis: genesis, Ethash: ethash.Config{PowMode: ethash.ModeFake}})
//if err != nil {
//t.Fatalf("can't create new ethereum service: %v", err)
//}
//if err := n.Start(); err != nil {
//t.Fatalf("can't start test node: %v", err)
//}
//if _, err := ethservice.BlockChain().InsertChain(blocks[1:5]); err != nil {
//t.Fatalf("can't import test blocks: %v", err)
//}

//api := NewEth2API(ethservice)
//for i := 5; i < 10; i++ {
//var blockRLP bytes.Buffer
//rlp.Encode(&blockRLP, blocks[i])
//err := api.NewBlock(blockRLP.Bytes())
//if err != nil {
//t.Fatalf("Failed to insert block: %v", err)
//}
//}
//api.head = blocks[9].Hash()

//if ethservice.BlockChain().CurrentBlock().Hash() != blocks[9].Hash() {
//t.Fatalf("Wrong head")
//}

//for i := 0; i < 3; i++ {
//var blockRLP bytes.Buffer
//rlp.Encode(&blockRLP, forkedBlocks[i])
//err := api.NewBlock(blockRLP.Bytes())
//if err != nil {
//t.Fatalf("Failed to insert block: %v", err)
//}
//}

//api.SetHead(forkedBlocks[2].Hash())

//if ethservice.BlockChain().CurrentBlock().Hash() == forkedBlocks[2].Hash() {
//t.Fatalf("Wrong head after inserting fork %x != %x", blocks[9].Hash(), ethservice.BlockChain().CurrentBlock().Hash())
//}
//if api.head != forkedBlocks[2].Hash() {
//t.Fatalf("Registered wrong head")
//}
//}
