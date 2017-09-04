package blkchain

import "io"

type TxIn struct {
	PrevOut   OutPoint
	ScriptSig []byte
	Sequence  uint32
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
