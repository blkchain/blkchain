package coredb

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/blkchain/blkchain"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Block information stored in LevelDb.
type leveldbBlockHeader struct {
	Version VarInt32 // Qt/Core software version
	Height  VarInt32
	Status  VarInt32
	TxN     VarInt32
	FileN   VarInt32
	DataPos VarInt32
	UndoPos VarInt32
	blkchain.BlockHeader
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

func (ibh *leveldbBlockHeader) BinRead(r io.Reader) (err error) {
	if err := blkchain.BinRead(&ibh.Version, r); err != nil {
		return err
	}
	if err := blkchain.BinRead(&ibh.Height, r); err != nil {
		return err
	}
	if err := blkchain.BinRead(&ibh.Status, r); err != nil {
		return err
	}
	if err := blkchain.BinRead(&ibh.TxN, r); err != nil {
		return err
	}
	if (ibh.Status & (BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO)) != 0 {
		if err := blkchain.BinRead(&ibh.FileN, r); err != nil {
			return err
		}
	}
	if (ibh.Status & BLOCK_HAVE_DATA) != 0 {
		if err := blkchain.BinRead(&ibh.DataPos, r); err != nil {
			return err
		}
	}
	if (ibh.Status & BLOCK_HAVE_UNDO) != 0 {
		if err := blkchain.BinRead(&ibh.UndoPos, r); err != nil {
			return err
		}
	}
	if err = blkchain.BinRead(&ibh.BlockHeader, r); err != nil {
		return err
	}
	return nil
}

type levelDbBlockHeaderIndex struct {
	m          map[int][]*leveldbBlockHeader
	blocksPath string
	magic      uint32
	height     int // current height
	n          int // pos within height
	count      int
}

func (bi *levelDbBlockHeaderIndex) Next() bool {
	if len(bi.m) == 0 {
		return false
	}
	if bi.n < len(bi.m[bi.height])-1 {
		bi.n++
	} else {
		if len(bi.m[bi.height+1]) == 0 {
			return false
		}
		bi.height++
		bi.n = 0
	}
	return true
}

type VarInt32 uint32

func (v *VarInt32) BinRead(r io.Reader) error {
	if i, err := blkchain.ReadVarInt(r); err != nil {
		return err
	} else {
		*v = VarInt32(i)
		return nil
	}
}

func (bi *levelDbBlockHeaderIndex) blockHeader() *leveldbBlockHeader {
	if len(bi.m[bi.height]) > 0 {
		return bi.m[bi.height][bi.n]
	}
	return nil
}

func (bi *levelDbBlockHeaderIndex) BlockHeader() *blkchain.BlockHeader {
	bh := bi.blockHeader()
	if bh != nil {
		return &bh.BlockHeader
	}
	return nil
}

func (bi *levelDbBlockHeaderIndex) Count() int {
	return bi.count
}

func (bi *levelDbBlockHeaderIndex) CurrentHeight() int {
	return bi.height
}

func (bi *levelDbBlockHeaderIndex) Close() error {
	return nil
}

func (bi *levelDbBlockHeaderIndex) ReadBlock() (*blkchain.Block, error) {

	ibh := bi.blockHeader()

	path := filepath.Join(bi.blocksPath, fmt.Sprintf("%s%05d.dat", "blk", int(ibh.FileN)))
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Opening file %v: %v", path, err)
	}
	defer f.Close()

	pos := int64(ibh.DataPos) - 8 // magic + size = 8
	if _, err := f.Seek(pos, 0); err != nil {
		return nil, fmt.Errorf("Seeking to pos %d in file %v: %v", pos, path, err)
	}

	b := blkchain.Block{Magic: bi.magic}
	if err := blkchain.BinRead(&b, f); err != nil {
		return nil, fmt.Errorf("Reading block: %v", err)
	}

	return &b, nil
}

// Returns a levelDbBlockHeaderIndex over which we can iterate with
// Next(). Some heights have multiple blocks. This func removes
// orphans (TODO - why do we, it caused a problem with marking spends,
// but that should be no longer an issue since we import UTXO set
// separately?) Core cannot be running during this (TODO why?), but as
// soon as it is done you should be able to start it back up.
func ReadLevelDbBlockHeaderIndex(path, blocksPath string, magic uint32, startHeight int) (blkchain.BlockHeaderIndex, error) {

	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result := &levelDbBlockHeaderIndex{
		m:          make(map[int][]*leveldbBlockHeader, 500000),
		height:     startHeight,
		blocksPath: blocksPath,
		magic:      magic,
	}

	iter := db.NewIterator(util.BytesPrefix([]byte("b")), nil)
	for iter.Next() {

		var bh leveldbBlockHeader
		if err := blkchain.BinRead(&bh, bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}

		if (bh.Status & BLOCK_VALID_CHAIN) != BLOCK_VALID_CHAIN {
			continue
		}

		if list, ok := result.m[int(bh.Height)]; !ok {
			result.m[int(bh.Height)] = []*leveldbBlockHeader{&bh}
		} else {
			result.m[int(bh.Height)] = append(list, &bh)
		}
		result.count++
	}

	log.Printf("Read %d block header entries.", result.count)

	// Eliminate orphans
	count, err := eliminateOrphans(result.m)
	if err != nil {
		return nil, err
	}
	result.count = count

	return result, nil
}

type ChainStateReader struct {
	*leveldb.DB
	iterator.Iterator
}

func NewChainStateChecker(path string) (*ChainStateReader, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &ChainStateReader{db, nil}, nil
}

func (r *ChainStateReader) IsUTXO(hash blkchain.Uint256, n uint32) (bool, error) {
	var buf [40]byte
	buf[0] = 'C'
	w := bytes.NewBuffer(buf[:1])
	if err := blkchain.BinWrite(&DbOutPoint{hash, n}, w); err != nil {
		return false, err
	}
	return r.Has(w.Bytes(), nil)
}

// TODO unused
func NewChainStateIterator(path string) (*ChainStateReader, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	iter := db.NewIterator(util.BytesPrefix([]byte("C")), nil)
	return &ChainStateReader{db, iter}, nil
}

// TODO unused
func (r *ChainStateReader) GetUTXO() (*UTXO, error) {
	var u UTXO
	if err := blkchain.BinRead(&u.DbOutPoint, bytes.NewReader(r.Key()[1:])); err != nil {
		return nil, err
	}
	if err := blkchain.BinRead(&u, bytes.NewReader(r.Value())); err != nil {
		return nil, err
	}
	return &u, nil
}

// Eliminate orphans by walking the chan backwards and whenever we
// see more than one block at a height, picking the one that
// matches its descendant's PrevHash.
func eliminateOrphans(m map[int][]*leveldbBlockHeader) (int, error) {

	minHeight, maxHeight, count := -1, -1, 0

	// Find min, max and count
	for h, v := range m {
		if minHeight > h || minHeight == -1 {
			minHeight = h
		}
		if maxHeight < h || maxHeight == -1 {
			maxHeight = h
		}
		count += len(v)
	}

	// It is possible that we are at a split, i.e. more than block
	// exists at max height. In this specific case (levelDb import),
	// we can just delete them until the main chain unity is found.
	for h := maxHeight; len(m[h]) > 1 && h >= minHeight; h-- {
		log.Printf("Chain is split at heighest height, ignoring height %d", h)
		delete(m, h)
		maxHeight--
	}

	prevHash := m[maxHeight][0].PrevHash
	for h := maxHeight - 1; h >= minHeight; h-- {
		if len(m[h]) > 1 { // More than one block at this height
			for _, bh := range m[h] {
				if bh.Hash() == prevHash {
					m[h] = []*leveldbBlockHeader{bh}
				} else {
					log.Printf("Ignoring orphan block %v", bh.Hash())
					count--
				}
			}
			if len(m[h]) != 1 {
				return count, fmt.Errorf("Problem finding valid parent when eliminating orphans.")
			}
		}

		if len(m[h]) > 0 {
			prevHash = m[h][0].PrevHash
		}
	}

	return count, nil
}
