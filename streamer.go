package blkchain

import "log"

// When scanning blocks, we need to be expecting that some blocks are
// orphan, as nodes that receive blocks that eventually are orphaned
// do store them forever. Not all sets of *.blk files are identical,
// their content depends on how they were obtained, and the network
// does relay orphaned blocks simply because when they first appear
// there is no way to tell that they are orphaned, this can only be
// inferred from subsequent blocks. So if you run a full node for any
// significant length of time, it most likely has a couple of orphaned
// blocks.

// The way we accomplish this is a little unusual: we have a
// queue-like structure, which is a fixed-size buffer of blocks. While
// blocks sit in this buffer, they can be modified. Separately we
// maintain a graph of these blocks in blkGraph, and it determines
// that blocks are rophaned and the correct heigth. The reason we need
// the buffer is so that we can modify the block height and orphan
// status before we send it to the database, and the orphan status is
// determined by the number of children a block has (chain length). It
// looks like keeping 10 blocks is enough for most situations.

func newBlockStream(out chan<- *blockRec, size int) chan<- *blockRec {
	in := make(chan *blockRec)
	go blockStreamWorker(in, out, size)
	return in
}

func blockStreamWorker(in <-chan *blockRec, out chan<- *blockRec, size int) {

	trail := blockRecQueue{}
	graph := newBlkGraph(size)

	type retryCnt struct {
		br  *blockRec
		cnt int
	}

	retry := make(map[Uint256]*retryCnt)
	zzzmax := 0

	for br := range in {
		if br == nil {
			out <- nil // db flush signal send right through
		} else {
			// this sets the height and orphan
			err := graph.add(br)
			if err != nil {
				// block can be out of order, we will retry up to size times
				retry[br.hash] = &retryCnt{br, 0}
				log.Printf("ZZZ Setting aside block (count: %d) %v", len(retry), br.hash)
			}

			if len(retry) > zzzmax {
				zzzmax = len(retry)
			}

			for _, r := range retry {
				if err := graph.add(r.br); err != nil {
					r.cnt++
					// if r.cnt > 2000 {
					// 	log.Printf("ZZZ Invalid block %v: %v", r.br.hash, err)
					// 	delete(retry, r.br.hash)
					// }
				} else {
					delete(retry, r.br.hash)
					log.Printf("ZZZ SUCCESS!!! len(retry): %d max(len): %d: %v", len(retry), zzzmax, br.hash)
				}
			}

			if err == nil {
				trail.push(br)
				for trail.size() >= size {
					out <- trail.pop()
				}
			}
		}
	}

	for trail.size() > 0 {
		out <- trail.pop()
	}

	log.Printf("ZZZ MAX RETRY: %d", zzzmax)
	log.Printf("ZZZ final RETRY len: %d", len(retry))

	close(out)
}

type blockRecQueue []*blockRec

// fifo push (yes, these must be pointer methods)
func (q *blockRecQueue) push(n *blockRec) {
	// 	*q = append(blockRecQueue{n}, *q...) // filo
	*q = append(*q, n) // fifo
}

func (q *blockRecQueue) pop() (n *blockRec) {
	if len(*q) == 0 {
		return nil
	}
	n, *q = (*q)[0], (*q)[1:]
	return n
}

func (q *blockRecQueue) size() int {
	return len(*q)
}
