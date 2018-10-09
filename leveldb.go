package blkchain

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type levelDbBlockHeaderIndex struct {
	m                    map[int][]*IdxBlockHeader
	blocksPath           string
	magic                uint32
	height, maxHeight, n int
	count                int
}

func (bi *levelDbBlockHeaderIndex) Next() bool {
	if len(bi.m) == 0 { // just in case
		return false
	}
	if bi.height > bi.maxHeight {
		return false
	}
	if bi.n < len(bi.m[bi.height])-1 {
		bi.n++
	} else {
		bi.height++
		bi.n = 0
	}
	return true
}

func (bi *levelDbBlockHeaderIndex) BlockHeader() *IdxBlockHeader {
	if len(bi.m[bi.height]) > 0 {
		return bi.m[bi.height][bi.n]
	}
	return nil
}

func (bi *levelDbBlockHeaderIndex) Count() int {
	return bi.count
}

func (bi *levelDbBlockHeaderIndex) CurrentHeight() int {
	return bi.height
}

func (bi *levelDbBlockHeaderIndex) ReadBlock() (*Block, error) {

	ibh := bi.BlockHeader()

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

	b := Block{Magic: bi.magic}
	if err := BinRead(&b, f); err != nil {
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
func ReadLevelDbBlockHeaderIndex(path, blocksPath string, magic uint32, startHeight int) (BlockHeaderIndex, error) {

	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result := &levelDbBlockHeaderIndex{
		m:          make(map[int][]*IdxBlockHeader, 500000),
		height:     startHeight,
		blocksPath: blocksPath,
		magic:      magic,
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

	log.Printf("Read %d block header entries, maxHeight: %d.", result.count, result.maxHeight)

	// Eliminate orphans
	mh, c, err := EliminateOrphans(result.m, result.maxHeight, result.count)
	if err != nil {
		return nil, err
	}
	result.maxHeight, result.count = mh, c

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
