package blkchain

import (
	"bytes"
	"fmt"
	"io"
)

type Tx struct {
	Version  uint32
	TxIns    TxInList
	TxOuts   TxOutList
	LockTime uint32
	SegWit   bool
}

func (tx *Tx) Hash() Uint256 {
	buf := new(bytes.Buffer)
	tx.binWriteWithoutWitness(buf)
	return ShaSha256(buf.Bytes())
}

func (tx *Tx) BinRead(r io.Reader) (err error) {
	var wcnt int

	if err = BinRead(&tx.Version, r); err != nil {
		return err
	}

	if err = BinRead(&tx.TxIns, r); err != nil {
		return err
	}

	if len(tx.TxIns) == 0 { // SegWit

		flag, err := readCompactSize(r)
		if err != nil {
			return err
		}
		if flag != 1 {
			return fmt.Errorf("Invalid SegWit flag: %d", flag)
		}

		if err = BinRead(&tx.TxIns, r); err != nil { // Read txins again
			return err
		}
		wcnt = len(tx.TxIns)
	}

	if err = BinRead(&tx.TxOuts, r); err != nil {
		return err
	}

	if wcnt > 0 { // Read witness
		for _, txin := range tx.TxIns {
			var wits Witness
			if err = BinRead(&wits, r); err != nil {
				return err
			}
			txin.Witness = wits
		}
		tx.SegWit = true
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
	if tx.SegWit {
		_, err = w.Write([]byte{0x00, 0x01})
	}
	if err = BinWrite(&tx.TxIns, w); err != nil {
		return err
	}
	if err = BinWrite(&tx.TxOuts, w); err != nil {
		return err
	}
	if tx.SegWit {
		for _, txin := range tx.TxIns {
			if err = BinWrite(&txin.Witness, w); err != nil {
				return err
			}
		}
	}
	if err = BinWrite(tx.LockTime, w); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) binWriteWithoutWitness(w io.Writer) (err error) {
	// This is for computing the txid, it is the transaction without
	// the segwit marker and without the witness data.
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
