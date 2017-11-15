package blkchain

import (
	"crypto/sha256"
	"fmt"
)

// We're sticking with value rather than pointer for now, we think it's
// faster and safer. TODO: is it?
type Uint256 [32]byte

func (u Uint256) String() (s string) {
	for i := 0; i < 32; i++ {
		s += fmt.Sprintf("%02x", u[31-i])
	}
	return
}

// NB: we interpret this as little-endian. Traditionally Bitcoin
// transaction ids are printed in big-endian, i.e. reverse of this.
func ShaSha256(b []byte) Uint256 {
	first := sha256.Sum256(b)
	return sha256.Sum256(first[:])
}

func uint256FromBytes(from []byte) Uint256 {
	var result Uint256
	copy(result[:], from)
	return result
}
