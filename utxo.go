package blkchain

import (
	"io"

	"github.com/btcsuite/btcd/btcec"
)

type UTXO struct {
	DbOutPoint
	Height   int
	Coinbase bool
	TxOut
}

// An OutPoint which uses a varint for N such as the case in
// chainstate LevelDb keys
type DbOutPoint OutPoint

func (o *DbOutPoint) BinRead(r io.Reader) error {
	if err := BinRead(&o.Hash, r); err != nil {
		return err
	}
	if n, err := readVarInt(r); err != nil {
		return err
	} else {
		o.N = uint32(n)
	}
	return nil
}

func (o *DbOutPoint) BinWrite(w io.Writer) error {
	if err := BinWrite(o.Hash, w); err != nil {
		return err
	}
	if err := writeVarInt(uint64(o.N), w); err != nil {
		return err
	}
	return nil
}

// Note that the OutPoint is not read here
func (u *UTXO) BinRead(r io.Reader) (err error) {

	// https://github.com/bitcoin/bitcoin/blob/0.15/src/coins.h#L67

	// Height and CoinBase
	var code varInt32
	if err := BinRead(&code, r); err != nil {
		return err
	}
	u.Height = int(code >> 1)
	u.Coinbase = (code & 1) != 0

	// Value: a riddle wrapped in an enigma. This a compressed integer
	// stored as a varint, very interesting.
	if vv, err := readVarInt(r); err != nil {
		return err
	} else {
		u.Value = int64(decompressAmount(vv))
	}

	// Size
	var vs varInt32
	if err := BinRead(&vs, r); err != nil {
		return err
	}

	// https://github.com/bitcoin/bitcoin/blob/0.15/src/compressor.h#L71
	const specialScripts = 6
	if vs < specialScripts {
		buf := make([]byte, getSpecialSize(vs))
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return err
		}
		u.ScriptPubKey = decompressScript(int(vs), buf)
		return nil
	} else {
		// TODO: We are not checking MAX_SCRIPT_SIZE here because this
		// is coming from Core. Otherwise, we should.
		buf := make([]byte, vs-specialScripts)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return err
		}
		u.ScriptPubKey = buf
	}
	return nil
}

// This is copied from C++
// https://github.com/bitcoin/bitcoin/blob/0.15/src/compressor.cpp#L161
func decompressAmount(x uint64) uint64 {
	if x == 0 {
		return 0
	}
	x--
	e := x % 10
	x /= 10
	var n uint64
	if e < 9 {
		d := (x % 9) + 1
		x /= 9
		n = x*10 + d
	} else {
		n = x + 1
	}
	for e != 0 {
		n *= 10
		e--
	}
	return n
}

// https://github.com/bitcoin/bitcoin/blob/0.15/src/compressor.cpp#L79
func getSpecialSize(size varInt32) int {
	if size == 0 || size == 1 {
		return 20
	}
	if size == 2 || size == 3 || size == 4 || size == 5 {
		return 32
	}
	return 0
}

// https://github.com/bitcoin/bitcoin/blob/0.15/src/compressor.cpp#L88
func decompressScript(size int, in []byte) []byte {
	switch size {
	case 0x00:
		script := make([]byte, 25)
		script[0] = 0x76 // OP_DUP
		script[1] = 0xa9 // OP_HASH160
		script[2] = 20
		copy(script[3:], in)
		script[23] = 0x88 // OP_EQUALVERIFY
		script[24] = 0xac // OP_CHECKSIG
		return script
	case 0x01:
		script := make([]byte, 23)
		script[0] = 0xa9 // OP_HASH160
		script[1] = 20
		copy(script[2:], in)
		script[22] = 0x87 // OP_EQUAL
		return script
	case 0x02, 0x03:
		script := make([]byte, 35)
		script[0] = 33
		script[1] = byte(size)
		copy(script[2:], in)
		script[34] = 0xac // OP_CHECKSIG
		return script
	case 0x04, 0x05:
		cKey := make([]byte, 33)
		cKey[0] = byte(size) - 2
		copy(cKey[1:], in)
		key, err := btcec.ParsePubKey(cKey, btcec.S256())
		if err != nil {
			return nil
		}
		script := make([]byte, 67)
		script[0] = 65
		copy(script[1:], key.SerializeUncompressed())
		script[66] = 0xac // OP_CHECKSIG
		return script
	}
	return nil
}
