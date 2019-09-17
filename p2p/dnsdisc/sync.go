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

package dnsdisc

import (
	"context"
	"crypto/ecdsa"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// clientTree is a full tree being synced.
type clientTree struct {
	c          *Client
	lastUpdate mclock.AbsTime // last revalidation of root
	loc        *linkEntry
	root       *rootEntry
	enrs       *subtreeSync
	links      *subtreeSync
}

func newClientTree(c *Client, loc *linkEntry) *clientTree {
	return &clientTree{c: c, loc: loc}
}

func (ct *clientTree) matchPubkey(key *ecdsa.PublicKey) bool {
	return keysEqual(ct.loc.pubkey, key)
}

func keysEqual(k1, k2 *ecdsa.PublicKey) bool {
	return k1.Curve == k2.Curve && k1.X.Cmp(k2.X) == 0 && k1.Y.Cmp(k2.Y) == 0
}

// syncAll retrieves all entries of the tree.
func (ct *clientTree) syncAll() error {
	if err := ct.updateRoot(); err != nil {
		return err
	}
	if err := ct.links.resolveAll(); err != nil {
		return err
	}
	if err := ct.enrs.resolveAll(); err != nil {
		return err
	}
	return nil
}

// syncRandom retrieves a single entry of the tree. The Node return value
// is non-nil if the entry was a node.
func (ct *clientTree) syncRandom(ctx context.Context) (*enode.Node, error) {
	// Re-check root, but don't check every call.
	if ct.rootUpdateDue() {
		if err := ct.updateRoot(); err != nil {
			return nil, err
		}
	}

	// Link tree sync has priority, run it to completion before syncing ENRs.
	if !ct.links.done() {
		err := ct.syncNextLink(ctx)
		return nil, err
	}

	// Sync next random entry in ENR tree. Once every node has been visited, we simply
	// start over. This is fine because entries are cached.
	if ct.enrs.done() {
		ct.enrs = newSubtreeSync(ct.c, ct.loc, ct.root.eroot, false)
	}
	return ct.syncNextRandomENR(ctx)
}

func (ct *clientTree) syncNextLink(ctx context.Context) error {
	hash := ct.links.missing[0]
	e, err := ct.links.resolveNext(ctx, hash)
	if err != nil {
		return err
	}
	ct.links.missing = ct.links.missing[1:]

	if le, ok := e.(*linkEntry); ok {
		// Found linked tree, add it to client.
		ct.c.addTree(le)
	}
	return nil
}

func (ct *clientTree) syncNextRandomENR(ctx context.Context) (*enode.Node, error) {
	hash := ct.enrs.missing[0] // TODO: random
	e, err := ct.enrs.resolveNext(ctx, hash)
	if err != nil {
		return nil, err
	}
	ct.enrs.missing = ct.enrs.missing[1:]
	if ee, ok := e.(*enrEntry); ok {
		return ee.node, nil
	}
	return nil, nil
}

// updateRoot ensures that the given tree has an up-to-date root.
func (ct *clientTree) updateRoot() error {
	ct.lastUpdate = ct.c.clock.Now()
	ctx, cancel := context.WithTimeout(context.Background(), ct.c.cfg.Timeout)
	defer cancel()
	root, err := ct.c.resolveRoot(ctx, ct.loc)
	if err != nil {
		return err
	}
	ct.root = &root

	// Invalidate subtrees if changed.
	if ct.links == nil || root.lroot != ct.links.root {
		ct.links = newSubtreeSync(ct.c, ct.loc, root.lroot, true)
	}
	if ct.enrs == nil || root.eroot != ct.enrs.root {
		ct.enrs = newSubtreeSync(ct.c, ct.loc, root.eroot, false)
	}
	return nil
}

// rootUpdateDue returns true when a root update is needed.
func (ct *clientTree) rootUpdateDue() bool {
	return ct.root == nil || time.Duration(ct.c.clock.Now()-ct.lastUpdate) > ct.c.cfg.RecheckInterval
}

// subtreeSync is the sync of an ENR or link subtree.
type subtreeSync struct {
	c       *Client
	loc     *linkEntry
	root    string
	missing []string // missing tree node hashes
	link    bool     // true if this sync is for the link tree
}

func newSubtreeSync(c *Client, loc *linkEntry, root string, link bool) *subtreeSync {
	return &subtreeSync{c, loc, root, []string{root}, link}
}

func (ts *subtreeSync) done() bool {
	return len(ts.missing) == 0
}

func (ts *subtreeSync) resolveAll() error {
	for !ts.done() {
		hash := ts.missing[0]
		ctx, cancel := context.WithTimeout(context.Background(), ts.c.cfg.Timeout)
		_, err := ts.resolveNext(ctx, hash)
		cancel()
		if err != nil {
			return err
		}
		ts.missing = ts.missing[1:]
	}
	return nil
}

func (ts *subtreeSync) resolveNext(ctx context.Context, hash string) (entry, error) {
	e, err := ts.c.resolveEntry(ctx, ts.loc.domain, hash)
	if err != nil {
		return nil, err
	}
	switch e := e.(type) {
	case *enrEntry:
		if ts.link {
			return nil, errENRInLinkTree
		}
	case *linkEntry:
		if !ts.link {
			return nil, errLinkInENRTree
		}
	case *subtreeEntry:
		ts.missing = append(ts.missing, e.children...)
	}
	return e, nil
}
