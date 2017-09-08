package blkchain

import (
	"bytes"
	"fmt"
	"io"
)

const (
	MainNetMagic = 0xd9b4bef9
	TestNetMagic = 0x0709110b
)

type Block struct {
	Magic uint32
	*BlockHeader
	Txs TxList
}

type BlockHeader struct {
	Version        uint32
	PrevHash       Uint256
	HashMerkleRoot Uint256
	Time           uint32
	Bits           uint32
	Nonce          uint32
}

func (bh *BlockHeader) Hash() Uint256 {
	buf := new(bytes.Buffer)
	BinWrite(bh, buf)
	return ShaSha256(buf.Bytes())
}

func (b *Block) BinRead(r io.Reader) error {
	m, err := readMagic(r)
	if err != nil {
		return err
	}

	if b.Magic > 0 && b.Magic != m {
		return fmt.Errorf("Bad magic: %d", m)
	}

	var size uint32
	err = BinRead(&size, r)
	if err != nil {
		return err
	}

	var bh BlockHeader
	err = BinRead(&bh, r)
	if err != nil {
		return err
	}
	b.BlockHeader = &bh

	err = BinRead(&b.Txs, r)
	if err != nil {
		return err
	}
	return nil
}

// TODO: BinWrite()
