package blkchain

import (
	"crypto/sha256"
	"encoding/json"
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

// TODO This may be wrong, do we want this here?
func (u Uint256) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// sql.Scanner so that pq can scan these values from postgres
func (u *Uint256) Scan(value interface{}) error {
	if b, ok := value.([]byte); !ok {
		return fmt.Errorf("Unexpected type: %T", value)
	} else {
		copy(u[:], b)
	}
	return nil
}

// NB: we interpret this as little-endian. Traditionally Bitcoin
// transaction ids are printed in big-endian, i.e. reverse of this.
func ShaSha256(b []byte) Uint256 {
	first := sha256.Sum256(b)
	return sha256.Sum256(first[:])
}

func Uint256FromBytes(from []byte) Uint256 {
	var result Uint256
	copy(result[:], from)
	return result
}
