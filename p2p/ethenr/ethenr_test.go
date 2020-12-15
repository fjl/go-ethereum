package ethenr

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

func TestSign(t *testing.T) {
	privkey, _ := crypto.GenerateKey()
	addr := common.HexToAddress("0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe")

	record := new(enr.Record)
	record.Set(enr.IP{127, 0, 0, 1})
	record.Set(enr.UDP(30303))
	err := SignEth(record, privkey, addr)
	if err != nil {
		t.Fatal(err)
	}

	// node, err := enode.New(EthID{}, record)
	// if err != nil {
	// 	t.Fatal("does not validate:", err)
	// }
	// fmt.Println(node)
}
