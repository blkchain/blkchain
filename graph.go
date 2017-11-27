package blkchain

import (
	"fmt"
	"log"
)

type blkNode struct {
	br       *blockRec
	children []*blkNode
}

type blkGraph struct {
	root   *blkNode
	byHash map[Uint256]*blkNode
	sz     int
	splits map[Uint256]bool
}

// The purpose of this graph is two-fold. First, it can compute the
// height of an incoming block by incrementing its parent. Second, it
// identifies the longest chain and marks blockRecs as orphan if
// necessary. The graph has a limited size.
func newBlkGraph(size int) *blkGraph {
	return &blkGraph{
		byHash: make(map[Uint256]*blkNode, size),
		sz:     size,
		splits: make(map[Uint256]bool),
	}
}

// Add a blockRec to the graph. If the prevhash cannot be found, it's
// an error. After adding, we run splitCheck() which will identify
// splits, if any, and mark nodes as orphans accordingly. Note that
// the situation can change at every add, and orphan flags can flip if
// one chain became longer. If after add() the size exceeds the
// allowed size, the top node (and its orphans, if any) is removed.
func (g *blkGraph) add(br *blockRec) error {
	node := &blkNode{br: br}
	if g.root == nil {
		g.root = node
	} else {
		prev, ok := g.byHash[br.block.PrevHash]
		if !ok {
			return fmt.Errorf("Invalid prevHash: %v", br.block.PrevHash)
		}
		prev.children = append(prev.children, node)
		br.height = prev.br.height + 1
	}
	g.byHash[br.hash] = node
	g.splitCheck()
	for len(g.byHash) > g.sz {
		g.deleteTop()
	}
	return nil
}

// Remove the root and replace it by its child. If the root had a split,
// remove all orphan nodes (assumes splitCheck has been called).
func (g *blkGraph) deleteTop() {
	if g.root == nil {
		return
	}
	if len(g.root.children) == 0 {
		delete(g.byHash, g.root.br.hash)
		delete(g.splits, g.root.br.hash)
		g.root = nil
		return
	}
	if len(g.root.children) == 1 {
		delete(g.byHash, g.root.br.hash)
		delete(g.splits, g.root.br.hash)
		g.root = g.root.children[0]
		return
	}
	if len(g.root.children) > 1 {
		delete(g.byHash, g.root.br.hash)
		delete(g.splits, g.root.br.hash)
		for _, child := range g.root.children {
			if child.br.orphan {
				g.dft(child, func(n *blkNode) {
					delete(g.byHash, n.br.hash)
					delete(g.splits, n.br.hash)
				})
			} else {
				g.root = child
			}
		}
	}
}

// Figure out the chain length starting at node, if a split is
// detected, pick the longest chain (recursive).
func (g *blkGraph) chainLen(node *blkNode) (result int) {
	if node == nil {
		return 0
	}
	maxChild := 0
	result++
	for _, child := range node.children {
		l := g.chainLen(child)
		if l > maxChild {
			maxChild = l
		}
	}
	return result + maxChild
}

// Depth-first (pre-order) traversal
func (g *blkGraph) dft(start *blkNode, action func(*blkNode)) {
	queue := make(blkNodeStack, 0)
	seen := make(map[Uint256]bool)

	queue.enqueue(start)
	for len(queue) > 0 {
		if n := queue.pop(); n != nil && !seen[n.br.hash] {
			nn := g.byHash[n.br.hash]
			action(nn)
			seen[nn.br.hash] = true
			for _, p := range nn.children {
				queue.enqueue(p)
			}
		}
	}
}

func (g *blkGraph) splitCheck() {
	g.dft(g.root, func(n *blkNode) {
		if n != nil && len(n.children) > 1 {
			maxChild, winIdx := 0, 0
			for i, child := range n.children {
				c := g.chainLen(child)
				if c > maxChild {
					maxChild = c
					winIdx = i
				}
			}
			// TODO proper action when they are equal?
			if !g.splits[n.br.hash] {
				log.Printf("Chain split detected at %v, winning child: %v", n.br.hash, n.children[winIdx].br.hash)
				g.splits[n.br.hash] = true // suppress further warnings
			}
			// set orphan flag
			for i, child := range n.children {
				if i == winIdx {
					g.dft(child, func(child *blkNode) { child.br.orphan = n.br.orphan })
				} else {
					g.dft(child, func(child *blkNode) { child.br.orphan = true })
				}
			}
		}
	})
}

// A simple queue for our dft
type blkNodeStack []*blkNode

// yes, these must be methods on the pointer

// stack push
func (q *blkNodeStack) push(n *blkNode) {
	*q = append(blkNodeStack{n}, *q...)
}

// queue push
func (q *blkNodeStack) enqueue(n *blkNode) {
	*q = append(*q, n)
}

func (q *blkNodeStack) pop() (n *blkNode) {
	if len(*q) == 0 {
		return nil
	}
	n, *q = (*q)[0], (*q)[1:]
	return n
}
