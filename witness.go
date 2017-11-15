package blkchain

import "io"

type WitnessItem []byte

type Witness []WitnessItem

func (wits *Witness) BinRead(r io.Reader) error {
	return readList(r, func(r io.Reader) error {
		var wit WitnessItem
		wit, err := readString(r)
		if err != nil {
			return err
		}
		*wits = append(*wits, wit)
		return nil
	})
}

func (wits *Witness) BinWrite(w io.Writer) error {
	return writeList(w, len(*wits), func(r io.Writer, i int) error {
		return writeString((*wits)[i], w)
	})
}
