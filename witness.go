package blkchain

import "io"

type WitnessItem []byte

func (wi WitnessItem) Size() int {
	return compactSizeSize(uint64(len(wi))) + len(wi)
}

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

func (wits *Witness) Size() int {
	if len(*wits) == 0 { // non-segwit transaction
		return 0
	}
	result := compactSizeSize(uint64(len(*wits)))
	for _, w := range *wits {
		result += w.Size()
	}
	return result
}
