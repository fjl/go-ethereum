package ethenr

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

type EthAddr [20]byte

func (EthAddr) ENRKey() string { return "ea" }

// s256raw is an unparsed secp256k1 public key entry.
type s256raw []byte

func (s256raw) ENRKey() string { return "secp256k1" }

type EthID struct{}

func (EthID) Verify(r *enr.Record, sig []byte) error {
	var entry s256raw
	if err := r.Load(&entry); err != nil {
		return err
	} else if len(entry) != 33 {
		fmt.Printf("%d %x\n", len(entry), entry)
		return fmt.Errorf("invalid public key")
	}

	var ea EthAddr
	if err := r.Load(&ea); err != nil {
		return fmt.Errorf("invalid eth address in record: %v", err)
	}

	h := sha3.NewLegacyKeccak256()
	rlp.Encode(h, r.AppendElements(nil))
	if !crypto.VerifySignature(entry, h.Sum(nil), sig) {
		return enr.ErrInvalidSig
	}
	return nil
}

func (EthID) NodeAddr(r *enr.Record) []byte {
	var ea EthAddr
	r.Load(&ea)
	var addr = make([]byte, 32)
	copy(addr, ea[:])
	return addr
}

// SignEth signs a record using the "eth" scheme.
func SignEth(r *enr.Record, privkey *ecdsa.PrivateKey, addr common.Address) error {
	cpy := *r // copy record to prevent modifying it when there is a signing error

	// Set ID scheme name, pubkey and eth address kv pairs in record.
	pubkey := crypto.CompressPubkey(&privkey.PublicKey)
	cpy.Set(enr.ID("eth"))
	cpy.Set(s256raw(pubkey))
	cpy.Set(EthAddr(addr))

	// Encode and sign record.
	h := sha3.NewLegacyKeccak256()
	rlp.Encode(h, cpy.AppendElements(nil))
	sig, err := crypto.Sign(h.Sum(nil), privkey)
	if err != nil {
		return err
	}
	sig = sig[:len(sig)-1] // remove v
	if err = cpy.SetSig(EthID{}, sig); err == nil {
		*r = cpy
	}
	return err
}
