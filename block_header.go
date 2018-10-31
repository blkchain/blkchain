package blkchain

import (
	"bytes"
)

type BlockHeader struct {
	Version        Uint32
	PrevHash       Uint256
	HashMerkleRoot Uint256 `db:"merkleroot"`
	Time           Uint32
	Bits           Uint32
	Nonce          Uint32
}

func (bh *BlockHeader) Hash() Uint256 {
	buf := new(bytes.Buffer)
	BinWrite(bh, buf)
	return ShaSha256(buf.Bytes())
}

func (bh *BlockHeader) Size() int {
	return 4 + 32 + 32 + 4 + 4 + 4
}

type BlockHeaderIndex interface {
	Count() int
	CurrentHeight() int
	Next() bool
	BlockHeader() *BlockHeader
	ReadBlock() (*Block, error)
	Close() error
}
