package blkchain

import (
	"bytes"
	"fmt"
	"io"
	"log"
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
	Next() bool
	BlockHeader() *IdxBlockHeader
	ReadBlock() (*Block, error)
	Count() int
	CurrentHeight() int
}

type VarInt32 uint32

func (v *VarInt32) BinRead(r io.Reader) error {
	if i, err := readVarInt(r); err != nil {
		return err
	} else {
		*v = VarInt32(i)
		return nil
	}
}

// Block information stored in LevelDb.
type IdxBlockHeader struct {
	Version VarInt32 // Qt/Core software version
	Height  VarInt32
	Status  VarInt32
	TxN     VarInt32
	FileN   VarInt32
	DataPos VarInt32
	UndoPos VarInt32
	BlockHeader
}

// https://github.com/bitcoin/bitcoin/blob/0.15/src/chain.h#L125
const (
	BLOCK_VALID_HEADER       = 1
	BLOCK_VALID_TREE         = 2
	BLOCK_VALID_TRANSACTIONS = 3
	BLOCK_VALID_CHAIN        = 4
	BLOCK_VALID_SCRIPTS      = 5
	BLOCK_VALID_MASK         = BLOCK_VALID_HEADER | BLOCK_VALID_TREE | BLOCK_VALID_TRANSACTIONS | BLOCK_VALID_CHAIN | BLOCK_VALID_SCRIPTS
	BLOCK_HAVE_DATA          = 8
	BLOCK_HAVE_UNDO          = 16
	BLOCK_HAVE_MASK          = BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO
	BLOCK_FAILED_VALID       = 32
	BLOCK_FAILED_CHILD       = 64
	BLOCK_FAILED_MASK        = BLOCK_FAILED_VALID | BLOCK_FAILED_CHILD
	BLOCK_OPT_WITNESS        = 128
)

func (ibh *IdxBlockHeader) BinRead(r io.Reader) (err error) {
	if err := BinRead(&ibh.Version, r); err != nil {
		return err
	}
	if err := BinRead(&ibh.Height, r); err != nil {
		return err
	}
	if err := BinRead(&ibh.Status, r); err != nil {
		return err
	}
	if err := BinRead(&ibh.TxN, r); err != nil {
		return err
	}
	if (ibh.Status & (BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO)) != 0 {
		if err := BinRead(&ibh.FileN, r); err != nil {
			return err
		}
	}
	if (ibh.Status & BLOCK_HAVE_DATA) != 0 {
		if err := BinRead(&ibh.DataPos, r); err != nil {
			return err
		}
	}
	if (ibh.Status & BLOCK_HAVE_UNDO) != 0 {
		if err := BinRead(&ibh.UndoPos, r); err != nil {
			return err
		}
	}
	if err = BinRead(&ibh.BlockHeader, r); err != nil {
		return err
	}
	return nil
}

// Eliminate orphans by walking the chan backwards and whenever we
// see more than one block at a height, picking the one that
// matches its descendant's PrevHash.
func EliminateOrphans(m map[int][]*IdxBlockHeader, maxHeight, count int) (_maxHeight, _count int, err error) {
	// Find minHeight
	minHeight := maxHeight
	for k, _ := range m {
		if minHeight > k {
			minHeight = k
		}
	}

	if len(m[maxHeight]) > 1 {
		return 0, 0, fmt.Errorf("Chain is presently at a split, cannot continue.")
	}
	prevHash := m[maxHeight][0].PrevHash
	for h := maxHeight - 1; h > minHeight; h-- {
		if len(m[h]) > 1 {
			for _, bh := range m[h] {
				if bh.Hash() == prevHash {
					m[h] = []*IdxBlockHeader{bh}
				} else {
					log.Printf("Ignoring orphan block %v", bh.Hash())
					count--
				}
			}
			if len(m[h]) != 1 {
				return 0, 0, fmt.Errorf("Problem finding valid parent when eliminating orphans.")
			}
		}
		if len(m[h]) > 0 {
			prevHash = m[h][0].PrevHash
		} else {
			// It's possible we're missing a block. In which case reduce maxHeight by -2 (yes)
			if h < maxHeight {
				log.Printf("No block header at height %d, reducing maxHeight to: %d", h, h-2)
				maxHeight = h - 2
			}
		}
	}
	return maxHeight, count, nil
}
