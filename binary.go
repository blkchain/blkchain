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

func readCompactSize(r io.Reader) (uint64, error) {
	var buf [8]byte

	n, err := io.ReadFull(r, buf[:1])
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

func compactSizeSize(i uint64) int {
	// https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer
	switch {
	case i < 0xfd:
		return 1
	case i < math.MaxUint16:
		return 3
	case i < math.MaxUint32:
		return 5
	}
	return 9
}

func writeCompactSize(i uint64, w io.Writer) (err error) {
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
	size, err := readCompactSize(r)
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
	if err = writeCompactSize(uint64(len(s)), w); err != nil {
		return err
	}
	_, err = w.Write(s)
	return err
}

func readList(r io.Reader, doRead func(io.Reader) error) error {
	size, err := readCompactSize(r)
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
	err := writeCompactSize(uint64(size), w)
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

// This is equivalent of serialize.h:ReadVarInt()
// https://github.com/bitcoin/bitcoin/blob/0.15/src/serialize.h#L334
// The origin of this encoding is not clear to me, it is similar to LEB128.
// More info here: https://bitcoin.stackexchange.com/questions/51620/cvarint-serialization-format/51623
func ReadVarInt(r io.Reader) (uint64, error) {
	var n uint64
	for {
		var buf [1]byte
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return 0, err
		}
		// TODO - overflow check
		n = (n << 7) | uint64(buf[0]&0x7F)
		if (buf[0] & 0x80) != 0 {
			// TODO - overflow check
			n++
		} else {
			return n, nil
		}

	}
	panic("unreachable")
	return 0, nil
}

// https://github.com/bitcoin/bitcoin/blob/0.15/src/serialize.h#L317
func WriteVarInt(n uint64, w io.Writer) (err error) {
	buf := make([]byte, 10)
	len := 0
	for {
		if len == 0 {
			buf[len] = byte(n) & 0x7F
		} else {
			buf[len] = byte(n)&0x7F | 0x80
		}
		if n <= 0x7F {
			break
		}
		n = (n >> 7) - 1
		len++
	}
	for i := len; i >= 0; i-- {
		if _, err := w.Write([]byte{byte(buf[i])}); err != nil {
			return err
		}
	}
	return nil
}
