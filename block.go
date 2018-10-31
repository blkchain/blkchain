package blkchain

import (
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

func (b *Block) BaseSize() int {
	return b.BlockHeader.Size() + b.Txs.BaseSize()
}

func (b *Block) Size() int {
	return b.BlockHeader.Size() + b.Txs.Size()
}

func (b *Block) Weight() int {
	return b.BaseSize()*3 + b.Size()
}

func (b *Block) VirtualSize() int {
	return b.BlockHeader.Size() + b.Txs.VirtualSize()
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
