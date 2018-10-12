package btcnode

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/blkchain/blkchain"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire"
)

type btcNode struct {
	*peer.Peer
	tmout     time.Duration
	headersCh chan []*wire.BlockHeader
	blockCh   chan *wire.MsgBlock

	startHash            blkchain.Uint256
	height, maxHeight, n int
	count                int
	byHeight             map[int][]*blkchain.BlockHeader
}

type heightBH struct {
	height int
	bh     *blkchain.BlockHeader
}

func (b *btcNode) Count() int {
	return b.count
}

func (b *btcNode) CurrentHeight() int {
	return b.height
}

func (b *btcNode) Next() bool {

	if len(b.byHeight) == 0 { // just in case
		return false
	}
	if b.height > b.maxHeight {
		return false
	}
	if b.n < len(b.byHeight[b.height])-1 {
		b.n++
	} else {
		b.height++
		b.n = 0
	}

	return true
}

func (b *btcNode) BlockHeader() *blkchain.BlockHeader {
	if len(b.byHeight[b.height]) > 0 {
		return b.byHeight[b.height][b.n]
	}
	return nil
}

func (b *btcNode) ReadBlock() (*blkchain.Block, error) {
	bh := b.BlockHeader()
	return b.getBlock(bh.Hash())
}

func (b *btcNode) getHeaders() error {

	b.headersCh = make(chan []*wire.BlockHeader)

	byPrevHash := make(map[blkchain.Uint256][]*heightBH, 2000)

	lastHash := chainhash.Hash(b.startHash)
	for {

		// This avoids a subtle bug whereby we pass a pointer to an
		// array which later modify, and since it's a point it gets
		// modified where we passed it to as well.
		lhCopy := lastHash
		b.PushGetHeadersMsg(blockchain.BlockLocator{&lhCopy}, &chainhash.Hash{})

		var hdrs []*wire.BlockHeader
		select {
		case hdrs = <-b.headersCh:
		case <-time.After(b.tmout):
			return fmt.Errorf("Time out.")
		}

		if len(hdrs) == 0 { // No more headers
			log.Printf("No more headers.")
			break
		} else {
			log.Printf("Received batch of %d headers.", len(hdrs))
		}

		for _, h := range hdrs {
			bh := &blkchain.BlockHeader{
				Version:        uint32(h.Version),
				PrevHash:       blkchain.Uint256(h.PrevBlock),
				HashMerkleRoot: blkchain.Uint256(h.MerkleRoot),
				Time:           uint32(h.Timestamp.Unix()),
				Bits:           h.Bits,
				Nonce:          h.Nonce,
			}
			if list, ok := byPrevHash[bh.PrevHash]; !ok {
				byPrevHash[bh.PrevHash] = []*heightBH{&heightBH{0, bh}}
			} else {
				byPrevHash[bh.PrevHash] = append(list, &heightBH{0, bh})
			}
			b.count++

			lastHash = chainhash.Hash(bh.Hash())
		}
	}

	// Assign heights
	setChildrenHeight(byPrevHash, b.startHash, b.height)

	if b.byHeight == nil {
		b.byHeight = make(map[int][]*blkchain.BlockHeader, len(byPrevHash))
	}

	for _, hbhs := range byPrevHash {
		for _, hbh := range hbhs {
			if list, ok := b.byHeight[hbh.height]; !ok {
				b.byHeight[hbh.height] = []*blkchain.BlockHeader{hbh.bh}
			} else {
				b.byHeight[hbh.height] = append(list, hbh.bh)
			}
			if b.maxHeight < hbh.height {
				b.maxHeight = hbh.height
			}
		}
	}

	mh, c, err := eliminateOrphans(b.byHeight, b.maxHeight, b.count)
	if err != nil {
		return err
	}
	b.maxHeight, b.count = mh, c

	return nil

}

func txFromMsgTx(mtx *wire.MsgTx) *blkchain.Tx {
	tx := &blkchain.Tx{
		Version:  uint32(mtx.Version),
		TxIns:    make(blkchain.TxInList, 0, len(mtx.TxIn)),
		TxOuts:   make(blkchain.TxOutList, 0, len(mtx.TxOut)),
		LockTime: uint32(mtx.LockTime),
		SegWit:   false,
	}
	// TxIns
	for _, in := range mtx.TxIn {
		txin := &blkchain.TxIn{
			PrevOut: blkchain.OutPoint{
				Hash: blkchain.Uint256(in.PreviousOutPoint.Hash),
				N:    in.PreviousOutPoint.Index,
			},
			ScriptSig: in.SignatureScript,
			Sequence:  in.Sequence,
			Witness:   make(blkchain.Witness, 0, len(in.Witness)),
		}
		for _, w := range in.Witness {
			txin.Witness = append(txin.Witness, w)
		}
		if !tx.SegWit && len(txin.Witness) > 0 {
			tx.SegWit = true
		}
		tx.TxIns = append(tx.TxIns, txin)
	}
	// TxOuts
	for _, out := range mtx.TxOut {
		tx.TxOuts = append(tx.TxOuts, &blkchain.TxOut{
			Value:        out.Value,
			ScriptPubKey: out.PkScript,
		})
	}
	return tx
}

func blockFromMsgBlock(mb *wire.MsgBlock, magic uint32) *blkchain.Block {
	blk := &blkchain.Block{
		Magic: magic,
		BlockHeader: &blkchain.BlockHeader{
			Version:        uint32(mb.Header.Version),
			PrevHash:       blkchain.Uint256(mb.Header.PrevBlock),
			HashMerkleRoot: blkchain.Uint256(mb.Header.MerkleRoot),
			Time:           uint32(mb.Header.Timestamp.Unix()),
			Bits:           mb.Header.Bits,
			Nonce:          mb.Header.Nonce,
		},
		Txs: make(blkchain.TxList, 0, len(mb.Transactions)),
	}
	for _, mtx := range mb.Transactions {
		blk.Txs = append(blk.Txs, txFromMsgTx(mtx))
	}
	return blk
}

// Get a block from the node
func (b *btcNode) getBlock(hash blkchain.Uint256) (*blkchain.Block, error) {

	if b.blockCh == nil {
		b.blockCh = make(chan *wire.MsgBlock)
	}

	gdmsg := wire.NewMsgGetData()
	gdmsg.AddInvVect(wire.NewInvVect(wire.InvTypeWitnessBlock, (*chainhash.Hash)(&hash)))
	b.QueueMessage(gdmsg, nil)

	var block *wire.MsgBlock
	select {
	case block = <-b.blockCh:
	case <-time.After(b.tmout):
		return nil, fmt.Errorf("Time out.")
	}

	return blockFromMsgBlock(block, blkchain.MainNetMagic), nil
}

// Recursively (from lowest height) assign height.
func setChildrenHeight(byPrevHash map[blkchain.Uint256][]*heightBH, parentHash blkchain.Uint256, parentHeight int) {
	for _, child := range byPrevHash[parentHash] {
		child.height = parentHeight + 1
		// log.Printf("%v %v", child.height, child.bh.Hash())
		setChildrenHeight(byPrevHash, child.bh.Hash(), parentHeight+1)
	}
}

func ReadBtcnodeBlockHeaderIndex(addr string, tmout time.Duration, startHash blkchain.Uint256, startHeight int) (blkchain.BlockHeaderIndex, error) {

	node, err := connectToNode(addr, tmout, startHash, startHeight)
	if err != nil {
		return nil, err
	}
	node.tmout = tmout

	// Get headers
	if err := node.getHeaders(); err != nil {
		return nil, err
	}

	return node, nil
}

func connectToNode(addr string, tmout time.Duration, startHash blkchain.Uint256, startHeight int) (*btcNode, error) {

	result := &btcNode{
		height:    startHeight,
		startHash: startHash,
	}

	verackCh := make(chan bool)
	peerCfg := &peer.Config{
		DisableRelayTx:   true,
		UserAgentName:    "blkchain", // User agent name to advertise.
		UserAgentVersion: "0.0.1",    // User agent version to advertise.
		ChainParams:      &chaincfg.MainNetParams,
		Services:         0,
		TrickleInterval:  time.Second * 10,
		Listeners: peer.MessageListeners{
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verackCh <- true
			},
			OnBlock: func(_ *peer.Peer, msg *wire.MsgBlock, buf []byte) {
				if result.blockCh != nil {
					result.blockCh <- msg
				}
			},
			OnHeaders: func(p *peer.Peer, msg *wire.MsgHeaders) {
				if result.headersCh != nil {
					result.headersCh <- msg.Headers
				}
			},
		},
	}

	p, err := peer.NewOutboundPeer(peerCfg, addr)
	if err != nil {
		return nil, err
	}

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		return nil, err
	}

	p.AssociateConnection(conn)

	select {
	case <-verackCh:
		// Verack pretty much means we are connected
	case <-time.After(tmout):
		p.Disconnect()
		return nil, fmt.Errorf("Connection timeout")
	}
	result.Peer = p

	return result, nil
}

// Eliminate orphans by walking the chan backwards and whenever we
// see more than one block at a height, picking the one that
// matches its descendant's PrevHash.
func eliminateOrphans(m map[int][]*blkchain.BlockHeader, maxHeight, count int) (_maxHeight, _count int, err error) {
	// Find minHeight
	minHeight := maxHeight
	for k, _ := range m {
		if minHeight > k {
			minHeight = k
		}
	}

	if len(m[maxHeight]) > 1 {
		return 0, 0, fmt.Errorf("Chain is presently at a split, cannot continue.")
	}
	prevHash := m[maxHeight][0].PrevHash
	for h := maxHeight - 1; h > minHeight; h-- {
		if len(m[h]) > 1 {
			for _, bh := range m[h] {
				if bh.Hash() == prevHash {
					m[h] = []*blkchain.BlockHeader{bh}
				} else {
					log.Printf("Ignoring orphan block %v", bh.Hash())
					count--
				}
			}
			if len(m[h]) != 1 {
				return 0, 0, fmt.Errorf("Problem finding valid parent when eliminating orphans.")
			}
		}
		if len(m[h]) > 0 {
			prevHash = m[h][0].PrevHash
		} else {
			// It's possible we're missing a block. In which case reduce maxHeight by -2 (yes)
			if h < maxHeight {
				log.Printf("No block header at height %d, reducing maxHeight to: %d", h, h-2)
				maxHeight = h - 2
			}
		}
	}
	return maxHeight, count, nil
}
