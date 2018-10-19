package blkchain

import "fmt"

type Uint32 uint32

// Satisfy sql.Scanner
func (u *Uint32) Scan(value interface{}) error {
	if i, ok := value.(int64); !ok {
		return fmt.Errorf("Unexpected type: %T", value)
	} else {
		*u = Uint32(i)
	}
	return nil
}
