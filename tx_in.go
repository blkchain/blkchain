package blkchain

import "io"

type OutPoint struct {
	Hash Uint256
	N    uint32
}

type TxIn struct {
	PrevOut   OutPoint
	ScriptSig []byte
	Sequence  uint32
	Witness   Witness
}

func (tin *TxIn) BaseSize() int {
	outpoint := 32 + 4
	scriptsig := compactSizeSize(uint64(len(tin.ScriptSig))) + len(tin.ScriptSig)
	sequence := 4
	return outpoint + scriptsig + sequence
}

func (tin *TxIn) Size() int {
	return tin.BaseSize() + tin.Witness.Size()
}

func (tin *TxIn) sigOpCount() int {
	// TODO
	return 1
}

func (tin *TxIn) BinRead(r io.Reader) (err error) {
	if err = BinRead(&tin.PrevOut, r); err != nil {
		return err
	}
	if tin.ScriptSig, err = readString(r); err != nil {
		return err
	}
	if err = BinRead(&tin.Sequence, r); err != nil {
		return err
	}
	return nil
}

func (tin *TxIn) BinWrite(w io.Writer) (err error) {
	if err = BinWrite(tin.PrevOut, w); err != nil {
		return err
	}
	if err = writeString(tin.ScriptSig, w); err != nil {
		return err
	}
	if err = BinWrite(tin.Sequence, w); err != nil {
		return err
	}
	return nil
}

type TxInList []*TxIn

func (tins *TxInList) BinRead(r io.Reader) error {
	return readList(r, func(r io.Reader) error {
		var txin TxIn
		if err := BinRead(&txin, r); err != nil {
			return err
		}
		*tins = append(*tins, &txin)
		return nil
	})
}

func (tins *TxInList) BinWrite(w io.Writer) error {
	return writeList(w, len(*tins), func(r io.Writer, i int) error {
		return BinWrite((*tins)[i], w)
	})
}

func (tins *TxInList) BaseSize() int {
	result := compactSizeSize(uint64(len(*tins)))
	for _, t := range *tins {
		result += t.BaseSize()
	}
	return result
}

func (tins *TxInList) Size() int {
	result := compactSizeSize(uint64(len(*tins)))
	for _, t := range *tins {
		result += t.Size()
	}
	return result
}
