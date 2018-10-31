package db

import "github.com/blkchain/blkchain"

// These types reflect the database structure.

type BlockRec struct {
	*blkchain.Block

	Id     int
	Height int
	Hash   blkchain.Uint256
	Orphan bool

	size     int
	baseSize int
	weight   int
	virtSize int
}

type txRec struct {
	id      int64
	blockId int
	n       int // position within block
	tx      *blkchain.Tx
	hash    blkchain.Uint256

	size     int
	baseSize int
	weight   int
	virtSize int

	sync chan bool
	dupe bool // already seen
}

type txInRec struct {
	txId    int64
	n       int
	txIn    *blkchain.TxIn
	idCache *txIdCache
	sync    chan bool
}

type txOutRec struct {
	txId  int64
	n     int
	txOut *blkchain.TxOut
	hash  blkchain.Uint256
	sync  chan bool
}

// type BlockInfo struct {
// 	*blkchain.Block
// 	Height int
// 	Sync   chan bool // `json:"-"` // ZZZ
// }
