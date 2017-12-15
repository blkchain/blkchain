package blkchain

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

type varInt32 uint32

func (v *varInt32) BinRead(r io.Reader) error {
	if i, err := readVarInt(r); err != nil {
		return err
	} else {
		*v = varInt32(i)
		return nil
	}
}

// Block information stored in LevelDb.
type IdxBlockHeader struct {
	Version varInt32 // Qt/Core software version
	Height  varInt32
	Status  varInt32
	TxN     varInt32
	FileN   varInt32
	DataPos varInt32
	UndoPos varInt32
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

func (ibh *IdxBlockHeader) ReadBlock(blocksPath string, magic uint32) (*Block, error) {

	path := filepath.Join(blocksPath, fmt.Sprintf("%s%05d.dat", "blk", int(ibh.FileN)))
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Opening file %v: %v", path, err)
	}
	defer f.Close()

	pos := int64(ibh.DataPos) - 8 // magic + size = 8
	if _, err := f.Seek(pos, 0); err != nil {
		return nil, fmt.Errorf("Seeking to pos %d in file %v: %v", pos, path, err)
	}

	b := Block{Magic: magic}
	if err := BinRead(&b, f); err != nil {
		return nil, fmt.Errorf("Reading block: %v", err)
	}

	return &b, nil
}
