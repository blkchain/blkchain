package blkchain

import "io"

type TxOut struct {
	Value        int64 // in Satoshis
	ScriptPubKey []byte
}

func (tout *TxOut) Size() int {
	return 8 + compactSizeSize(uint64(len(tout.ScriptPubKey))) + len(tout.ScriptPubKey)
}

func (tout *TxOut) BinRead(r io.Reader) (err error) {
	if err = BinRead(&tout.Value, r); err != nil {
		return err
	}
	if tout.ScriptPubKey, err = readString(r); err != nil {
		return err
	}
	return nil
}

func (tout *TxOut) BinWrite(w io.Writer) (err error) {
	if err = BinWrite(tout.Value, w); err != nil {
		return err
	}
	if err = writeString(tout.ScriptPubKey, w); err != nil {
		return err
	}
	return nil
}

type TxOutList []*TxOut

func (touts *TxOutList) BinRead(r io.Reader) error {
	return readList(r, func(r io.Reader) error {
		var txout TxOut
		if err := BinRead(&txout, r); err != nil {
			return err
		}
		*touts = append(*touts, &txout)
		return nil
	})
}

func (touts *TxOutList) BinWrite(w io.Writer) error {
	return writeList(w, len(*touts), func(r io.Writer, i int) error {
		return BinWrite((*touts)[i], w)
	})
}

func (touts *TxOutList) Size() int {
	result := compactSizeSize(uint64(len(*touts)))
	for _, t := range *touts {
		result += t.Size()
	}
	return result
}
