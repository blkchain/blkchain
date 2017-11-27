package blkchain

import (
	"fmt"
	"testing"
)

func Test_blkNodeStack(t *testing.T) {

	var stack blkNodeStack

	stack.push(&blkNode{})
	stack.push(&blkNode{})

	if len(stack) != 2 {
		t.Error("len(stack) != 2")
	}
}

func Test_blkGraph_childLen(t *testing.T) {

	var hash, prev Uint256

	g := newBlkGraph(10)

	for i := byte(1); i < 10; i++ {
		hash[0] = i

		br := &blockRec{
			block: &Block{
				BlockHeader: &BlockHeader{
					PrevHash: prev,
				},
			},
			hash: hash,
		}

		g.add(br)
		prev = hash

	}
	lastPrev := prev

	prev[0] = 3
	for i := byte(1); i < 10; i++ {
		hash[1] = i
		br := &blockRec{
			block: &Block{
				BlockHeader: &BlockHeader{
					PrevHash: prev,
				},
			},
			hash: hash,
		}

		g.add(br)
		prev = hash
	}

	prev[1] = 3
	for i := byte(1); i < 10; i++ {
		hash[2] = i
		br := &blockRec{
			block: &Block{
				BlockHeader: &BlockHeader{
					PrevHash: prev,
				},
			},
			hash: hash,
		}

		g.add(br)
		prev = hash
	}

	prev = lastPrev
	for i := byte(10); i < 20; i++ {
		hash[0] = i

		br := &blockRec{
			block: &Block{
				BlockHeader: &BlockHeader{
					PrevHash: prev,
				},
			},
			hash: hash,
		}

		g.add(br)
		prev = hash

	}

	// Traversal
	g.dft(g.root, func(n *blkNode) {
		fmt.Printf("Action hash: %v orph: %v\n", n.br.hash, n.br.orphan)
	})

	// Split check
	g.splitCheck()

	// Traversal
	g.dft(g.root, func(n *blkNode) {
		fmt.Printf("Action hash: %v orph: %v\n", n.br.hash, n.br.orphan)
	})

	// delete top
	for len(g.byHash) > 0 {
		g.deleteTop()
		g.dft(g.root, func(n *blkNode) {
			fmt.Printf("Action hash: %v orph: %v\n", n.br.hash, n.br.orphan)
		})
	}
}
