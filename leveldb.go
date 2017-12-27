package blkchain

import (
	"bytes"
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type BlockHeaderIndex struct {
	m                    map[int][]*IdxBlockHeader
	blocksPath           string
	height, maxHeight, n int
	count                int
}

func (bi *BlockHeaderIndex) Next() bool {
	if len(bi.m) == 0 { // just in case
		return false
	}
	if bi.n < len(bi.m[bi.height])-1 {
		bi.n++
	} else if bi.height < bi.maxHeight {
		bi.height++
		bi.n = 0
	} else {
		return false
	}
	return true
}

func (bi *BlockHeaderIndex) BlockHeader() *IdxBlockHeader {
	return bi.m[bi.height][bi.n]
}

func (bi *BlockHeaderIndex) Start(height int) {
	bi.height = height
}

func (bi *BlockHeaderIndex) Count() int {
	return bi.count
}

// Returns a BlockHeaderIndex over which we can iterate with
// Next(). Some heights have multiple blocks. This func removes
// orphans (TODO - why do we, it caused a problem with marking spends,
// but that should be no longer an issue since we import UTXO set
// separately?) Core cannot be running during this (TODO why?), but as
// soon as it is done you should be able to start it back up.
func ReadBlockHeaderIndex(path, blocksPath string) (*BlockHeaderIndex, error) {

	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result := &BlockHeaderIndex{
		m:          make(map[int][]*IdxBlockHeader, 500000),
		blocksPath: blocksPath,
	}

	iter := db.NewIterator(util.BytesPrefix([]byte("b")), nil)
	for iter.Next() {

		var bh IdxBlockHeader
		if err := BinRead(&bh, bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}

		if (bh.Status & BLOCK_VALID_CHAIN) != BLOCK_VALID_CHAIN {
			continue
		}

		if int(bh.Height) > result.maxHeight {
			result.maxHeight = int(bh.Height)
		}

		if list, ok := result.m[int(bh.Height)]; !ok {
			result.m[int(bh.Height)] = []*IdxBlockHeader{&bh}
		} else {
			result.m[int(bh.Height)] = append(list, &bh)
		}
		result.count++
	}

	// Eliminate orphans by walking the chan backwards and whenever we
	// see more than one block at a height, picking the one that
	// matches its descendant's PrevHash.
	if len(result.m[result.maxHeight]) > 1 {
		return nil, fmt.Errorf("Chain is presently at a split, cannot continue.")
	}
	prevHash := result.m[result.maxHeight][0].PrevHash
	for h := result.maxHeight - 1; h > 0; h-- {
		if len(result.m[h]) > 1 {
			for _, bh := range result.m[h] {
				if bh.Hash() == prevHash {
					result.m[h] = []*IdxBlockHeader{bh}
				} else {
					log.Printf("Ignoring orphan block %v", bh.Hash())
					result.count--
				}
			}
			if len(result.m[h]) != 1 {
				return nil, fmt.Errorf("Problem finding valid parent when eliminating orphans.")
			}
		}
		prevHash = result.m[h][0].PrevHash
	}

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

func (r *ChainStateReader) IsUTXO(hash Uint256, n uint32) (bool, error) {
	var buf [40]byte
	buf[0] = 'C'
	w := bytes.NewBuffer(buf[:1])
	if err := BinWrite(&DbOutPoint{hash, n}, w); err != nil {
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
	if err := BinRead(&u.DbOutPoint, bytes.NewReader(r.Key()[1:])); err != nil {
		return nil, err
	}
	if err := BinRead(&u, bytes.NewReader(r.Value())); err != nil {
		return nil, err
	}
	return &u, nil
}
