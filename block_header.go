package blkchain

import (
	"bytes"
)

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

type BlockHeaderIndex interface {
	Count() int
	CurrentHeight() int
	Next() bool
	BlockHeader() *BlockHeader
	ReadBlock() (*Block, error)
}
