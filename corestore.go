package blkchain

import (
	"bufio"
	"io"
)

func NewCoreStore(dir string, start int, magic uint32) (io.Reader, error) {
	fb, err := newFileBundle(dir, start)
	if err != nil {
		return nil, err
	}

	return bufio.NewReaderSize(fb, 64*1024), nil
}
