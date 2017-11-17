package blkchain

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type fileBundle struct {
	dir    string
	idx    int
	prefix string
	f      *os.File
}

func newFileBundle(dir string, start int) (*fileBundle, error) {
	fb := &fileBundle{
		dir:    dir,
		prefix: "blk",
		idx:    start,
	}
	if err := fb.next(); err != nil {
		return nil, err
	}
	return fb, nil
}

func (f *fileBundle) next() (err error) {
	if f.f != nil { // not first time
		f.f.Close()
		f.idx++
	}
	path := filepath.Join(f.dir, fmt.Sprintf("%s%05d.dat", f.prefix, f.idx))
	log.Printf("Scanning file: %v", path)
	f.f, err = os.Open(path)

	return err
}

func (f *fileBundle) Read(b []byte) (n int, err error) {
	x := 0
	for n < len(b) {
		i, err := f.f.Read(b[n:])
		n += i

		if err == io.EOF {
			if err = f.next(); err != nil {
				if strings.Contains(err.Error(), "no such file") {
					return n, io.EOF
				}
				return n, err
			}
		} else if err != nil {
			break
		}
		x++
	}
	return n, err
}

func (f *fileBundle) Close() error {
	if f != nil && f.f != nil {
		return f.f.Close()
	}
	return nil
}
