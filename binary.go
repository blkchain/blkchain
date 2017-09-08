package blkchain

import (
	"encoding/binary"
	"io"
	"math"
)

func readMagic(r io.Reader) (uint32, error) {
	var magic [4]byte

	for magic[0] == 0x00 {
		if n, err := io.ReadFull(r, magic[:1]); n < 1 {
			return 0, err
		}
	}
	if _, err := io.ReadFull(r, magic[1:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(magic[:]), nil
}

type BinReader interface {
	BinRead(io.Reader) error
}
type BinWriter interface {
	BinWrite(io.Writer) error
}

// BinRead will see if BinReader interface is provided, otherwise it
// falls back to LittleEndian binary.Read.
func BinRead(s interface{}, r io.Reader) error {
	if br, ok := s.(BinReader); ok {
		return br.BinRead(r)
	}
	return binary.Read(r, binary.LittleEndian, s)
}

// Similar to BinRead, check for BinWriter, defer to binary.Write.
func BinWrite(s interface{}, w io.Writer) error {
	if bw, ok := s.(BinWriter); ok {
		return bw.BinWrite(w)
	}
	return binary.Write(w, binary.LittleEndian, s)
}

func readVarInt(r io.Reader) (uint64, error) {
	var buf [8]byte

	n, err := r.Read(buf[:1])
	if err != nil {
		return 0, err
	}

	switch buf[0] {
	case 0xfd:
		n, err = io.ReadFull(r, buf[:2])
	case 0xfe:
		n, err = io.ReadFull(r, buf[:4])
	case 0xff:
		n, err = io.ReadFull(r, buf[:8])
	}
	if err != nil {
		return 0, err
	}

	var result uint64
	for i := 0; i < n; i++ {
		result |= uint64(buf[i]) << uint64(i*8)
	}
	return result, nil
}

func writeVarInt(i uint64, w io.Writer) (err error) {
	if i < 0xfd {
		_, err = w.Write([]byte{byte(i)})
		return err
	}
	if i < math.MaxUint16 {
		if _, err = w.Write([]byte{0xfd}); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint16(i))
	}
	if i < math.MaxUint32 {
		if _, err = w.Write([]byte{0xfe}); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint32(i))
	}
	if _, err = w.Write([]byte{0xff}); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, i)
}

func readString(r io.Reader) ([]byte, error) {
	size, err := readVarInt(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, int(size))
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func writeString(s []byte, w io.Writer) (err error) {
	if err = writeVarInt(uint64(len(s)), w); err != nil {
		return err
	}
	_, err = w.Write(s)
	return err
}

func readList(r io.Reader, doRead func(io.Reader) error) error {
	size, err := readVarInt(r)
	if err != nil {
		return err
	}

	for i := uint64(0); i < size; i++ {
		if err = doRead(r); err != nil {
			return err
		}
	}
	return nil
}

func writeList(w io.Writer, size int, doWrite func(io.Writer, int) error) error {
	err := writeVarInt(uint64(size), w)
	if err != nil {
		return err
	}

	for i := 0; i < size; i++ {
		if err = doWrite(w, i); err != nil {
			return err
		}
	}
	return nil
}
