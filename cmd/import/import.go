package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/blkchain/blkchain"
	"github.com/blkchain/blkchain/btcnode"
	"github.com/blkchain/blkchain/coredb"
	"github.com/blkchain/blkchain/db"
)

func main() {

	connStr := flag.String("connstr", "host=/var/run/postgresql dbname=blocks sslmode=disable", "Db connection string")
	nodeAddr := flag.String("nodeaddr", "", "Bitcoin node address")
	nodeTmout := flag.Int("nodetmout", 30, "Bitcoin node timeout in seconds")
	blocksPath := flag.String("blocks", "", "/path/to/blocks")
	indexPath := flag.String("index", "", "/path/to/blocks/index (levelDb)")
	chainStatePath := flag.String("chainstate", "", "/path/to/blocks/chainstate (levelDb UTXO set)")
	testNet := flag.Bool("testnet", false, "Use testnet magic")
	cacheSize := flag.Int("cache-size", 1024*1024*30, "Tx hashes to cache for pervout_tx_id")
	wait := flag.Bool("wait", false, "Keep on waiting for blocks from Bitcoin node")

	flag.Parse()

	if *blocksPath == "" && *nodeAddr == "" {
		log.Fatalf("-blocks or -nodeAddr required.")
	}

	if *blocksPath != "" && *nodeAddr != "" {
		log.Fatalf("-blocks and -nodeAddr are mutually exclusive")
	}

	if *wait && *nodeAddr == "" {
		log.Fatalf("wait can only be specified with nodeAddr")
	}

	if *indexPath == "" {
		*indexPath = filepath.Join(*blocksPath, "index")
	}

	if *chainStatePath == "" {
		*chainStatePath = filepath.Join(*blocksPath, "..", "chainstate")
	}

	var magic uint32
	if *testNet {
		magic = blkchain.TestNetMagic
	} else {
		magic = blkchain.MainNetMagic
	}

	if *nodeAddr != "" {
		// Get blocks from a node
		tmout := time.Duration(*nodeTmout) * time.Second
		processEverythingBtcNode(*connStr, *nodeAddr, tmout, *cacheSize, *wait)

	} else {
		// Get block from levelDb
		if err := setRLimit(1024); err != nil { // LevelDb opens many files!
			log.Printf("Error setting rlimit: %v", err)
			return
		}
		processEverythingLevelDb(*connStr, *blocksPath, *indexPath, *chainStatePath, magic, *cacheSize)
	}

}

func processEverythingBtcNode(dbconnect, addr string, tmout time.Duration, cacheSize int, wait bool) {

	// monitor ctrl-c
	interrupt := make(chan bool, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Printf("Interrupt, exiting scan loop...")
		signal.Stop(sigCh)
		interrupt <- true
	}()

	writer, err := db.NewPGWriter(dbconnect, cacheSize, nil)
	if err != nil {
		log.Printf("Error creating writer: %v", err)
		return
	}

	for {
		count, err := btcNodeCatchUp(writer, addr, tmout, cacheSize, interrupt)
		if err != nil {
			log.Printf("Error catching up from btc node: %v", err)
			return
		}

		if count == 0 {
			log.Printf("Node has no more new headers, catch up done.")
			break
		}
	}

	if wait {
		processEachNewBlock(writer, addr, tmout, interrupt)
	}

	log.Printf("Closing channel, waiting for workers to finish...")
	writer.Close()
	log.Printf("All done.")
}

func btcNodeCatchUp(writer *db.PGWriter, addr string, tmout time.Duration, cacheSize int, interrupt chan bool) (int, error) {

	lastHeight, lastHashes, err := writer.HeightAndHashes()
	if err != nil {
		return 0, err
	}

	log.Printf("Reading block headers from Node (%s)...", addr)
	bhs, err := btcnode.ReadBtcnodeBlockHeaderIndex(addr, tmout, lastHeight, lastHashes)
	if err != nil {
		return 0, err
	}

	log.Printf("Read %d block headers.", bhs.Count())
	if bhs.Count() == 0 {
		bhs.Close()
		return 0, nil // This is not an error
	}

	if err := processBlocks(writer, bhs, true, interrupt); err != nil {
		return 0, err
	}

	bhs.Close()

	return bhs.Count(), nil
}

func processEachNewBlock(writer *db.PGWriter, addr string, tmout time.Duration, interrupt chan bool) {

	log.Printf("Connecting to Node (%s)...", addr)
	node, err := btcnode.ConnectToNode(addr, tmout)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	blkCh := make(chan *blkchain.Block, 8)

	var blkChWg sync.WaitGroup

	go func() {
		blkChWg.Add(1)
		defer blkChWg.Done()
		for blk := range blkCh {

			bi := &db.BlockInfo{
				Block:  blk,
				Height: -1, // Means the DB layer will figure it out
				Sync:   make(chan bool),
			}

			writer.WriteBlockInfo(bi)
			log.Printf("Queued block %v for writing.", blk.Hash())

			<-bi.Sync

			go func() {
				log.Printf("Marking orphan blocks...")
				writer.SetOrphans()
				log.Printf("Marking orphan blocks done.")
			}()
		}
	}()

	for {
		log.Printf("Waiting for a block...")

		blk, err := node.WaitForBlock(interrupt)
		if err != nil {
			log.Fatalf("ERROR: %v", err)
		}

		log.Printf("Received a block: %v", blk.Hash())
		blkCh <- blk

		if len(interrupt) > 0 {
			close(blkCh)
			blkChWg.Wait()
			break
		}
	}
	log.Printf("Exiting processEachNewBlock().")
}

func processEverythingLevelDb(dbconnect, blocksPath, indexPath, chainStatePath string, magic uint32, cacheSize int) {

	utxo, err := coredb.NewChainStateChecker(chainStatePath)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}
	defer utxo.Close()

	writer, err := db.NewPGWriter(dbconnect, cacheSize, utxo)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	lastHeight, _, err := writer.HeightAndHashes()
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	var startHeight int
	if lastHeight > 0 {
		// lastHeight is correct, pgwriter will ignore the last block
		// starting with lastHeight (as opposed to lH-1) risks
		// skipping duplicates in case of a split
		startHeight = lastHeight - 1
		log.Printf("Starting with block height: %d", startHeight)
	}

	log.Printf("Reading block headers from LevelDb (%s)...", indexPath)
	bhs, err := coredb.ReadLevelDbBlockHeaderIndex(indexPath, blocksPath, magic, startHeight)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
		return
	}
	log.Printf("Read %d block headers.", bhs.Count())
	if bhs.Count() == 0 {
		log.Printf("Is Core running? Stop it first, and try again. Exiting.")
		return
	}

	// monitor ctrl-c
	interrupt := make(chan bool, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Printf("Interrupt, exiting scan loop...")
		signal.Stop(sigCh)
		interrupt <- true
	}()

	if err := processBlocks(writer, bhs, false, interrupt); err != nil {
		log.Printf("Error processing blocks: %v", err)
	}

	log.Printf("Closing channel, waiting for workers to finish...")
	writer.Close()
	log.Printf("All done.")
}

func processBlocks(writer *db.PGWriter, bhs blkchain.BlockHeaderIndex, sync bool, interrupt chan bool) error {
	for bhs.Next() {
		bh := bhs.BlockHeader()

		if bh == nil {
			log.Printf("EOF: (Nil block header at %d).", bhs.CurrentHeight())
			break
		}
		b, err := bhs.ReadBlock()
		if err != nil {
			log.Printf("Error: %v", err)
			break
		}

		bi := &db.BlockInfo{
			Block:  b,
			Height: int(bhs.CurrentHeight()),
		}

		if sync {
			bi.Sync = make(chan bool) // Let us know when done
		}

		writer.WriteBlockInfo(bi)
		if sync {
			<-bi.Sync // Wait for it to be commited
		}

		if len(interrupt) > 0 {
			break
		}
	}
	return nil
}

func setRLimit(required uint64) error {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}
	if rLimit.Cur < required {
		log.Printf("Setting open files rlimit of %d to %d.", rLimit.Cur, required)
		rLimit.Cur = required
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return err
		}
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return err
		}
		if rLimit.Cur < required {
			return fmt.Errorf("Could not change open files rlimit to: %d", required)
		}
	}
	return nil
}
