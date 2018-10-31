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

func (tx *Tx) BaseSize() int {
	if !tx.SegWit {
		return tx.Size()
	}
	version, locktime := 4, 4
	return version + tx.TxIns.BaseSize() + tx.TxOuts.Size() + locktime
}

func (tx *Tx) Size() int {
	version, locktime, segwit := 4, 4, 0
	if tx.SegWit {
		segwit = 2 // marker+flag
	}
	return version + segwit + tx.TxIns.Size() + tx.TxOuts.Size() + locktime
}

func (tx *Tx) Weight() int {
	return tx.BaseSize()*3 + tx.Size()
}

func (tx *Tx) sigOpCount() (count int) {
	for _, tin := range tx.TxIns {
		count += tin.sigOpCount()
	}
	return count
}

func (tx *Tx) VirtualSize() int {
	// TODO: This only an approximation
	// We need script parsing code for precise result
	const (
		witnessScaleFactor   = 4
		defaultBytesPerSigOp = 20
		// maxPubkeysPerMultisig = 20
	)

	sigOpWeight := tx.sigOpCount() * witnessScaleFactor * defaultBytesPerSigOp
	weight := tx.Weight()

	if weight < sigOpWeight {
		weight = sigOpWeight
	}

	return (weight + witnessScaleFactor - 1) / witnessScaleFactor
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

func (tl *TxList) BaseSize() int {
	result := compactSizeSize(uint64(len(*tl)))
	for _, t := range *tl {
		result += t.BaseSize()
	}
	return result
}

func (tl *TxList) Size() int {
	result := compactSizeSize(uint64(len(*tl)))
	for _, t := range *tl {
		result += t.Size()
	}
	return result
}

func (tl *TxList) Weight() int {
	result := compactSizeSize(uint64(len(*tl)))
	for _, t := range *tl {
		result += t.Weight()
	}
	return result
}

func (tl *TxList) VirtualSize() int {
	result := compactSizeSize(uint64(len(*tl)))
	for _, t := range *tl {
		result += t.VirtualSize()
	}
	return result
}
