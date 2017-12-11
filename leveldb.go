package blkchain

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Returns a map keyed on height. Some heights have multiple blocks.
// Core cannot be running during this (TODO why?), but as soon as it
// is done you should be able to start it back up.
func ReadBlockHeaderIndex(path string) (map[int][]*IdxBlockHeader, error) {

	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result := make(map[int][]*IdxBlockHeader, 500000)

	iter := db.NewIterator(util.BytesPrefix([]byte("b")), nil)

	for iter.Next() {

		var bh IdxBlockHeader
		if err := BinRead(&bh, bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}

		if (bh.Status & BLOCK_VALID_CHAIN) != BLOCK_VALID_CHAIN {
			continue
		}

		if list, ok := result[int(bh.Height)]; !ok {
			result[int(bh.Height)] = []*IdxBlockHeader{&bh}
		} else {
			result[int(bh.Height)] = append(list, &bh)
		}
	}

	return result, nil
}
