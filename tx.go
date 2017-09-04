package blkchain

import (
	"bytes"
	"io"
)

type Tx struct {
	Version  uint32
	TxIns    TxInList
	TxOuts   TxOutList
	LockTime uint32
}

type OutPoint struct {
	Hash Uint256
	N    uint32
}

func (tx *Tx) Hash() Uint256 {
	buf := new(bytes.Buffer)
	BinWrite(tx, buf)
	return ShaSha256(buf.Bytes())
}

func (tx *Tx) BinRead(r io.Reader) (err error) {
	if err = BinRead(&tx.Version, r); err != nil {
		return err
	}
	if err = BinRead(&tx.TxIns, r); err != nil {
		return err
	}
	if err = BinRead(&tx.TxOuts, r); err != nil {
		return err
	}
	if err = BinRead(&tx.LockTime, r); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) BinWrite(w io.Writer) (err error) {
	if err = BinWrite(tx.Version, w); err != nil {
		return err
	}
	if err = BinWrite(&tx.TxIns, w); err != nil {
		return err
	}
	if err = BinWrite(&tx.TxOuts, w); err != nil {
		return err
	}
	if err = BinWrite(tx.LockTime, w); err != nil {
		return err
	}
	return nil
}

type TxList []*Tx

func (tl *TxList) BinRead(r io.Reader) error {
	return readList(r, func(r io.Reader) error {
		var tx Tx
		if err := BinRead(&tx, r); err != nil {
			return err
		}
		*tl = append(*tl, &tx)
		return nil
	})
}
