package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

	flag.Parse()

	if *blocksPath == "" && *nodeAddr == "" {
		log.Fatalf("-blocks or -nodeAddr required.")
	}

	if *blocksPath != "" && *nodeAddr != "" {
		log.Fatalf("-blocks and -nodeAddr are mutually exclusive")
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
		processEverythingBtcNode(*connStr, *nodeAddr, tmout, *cacheSize)
	} else {
		// Get block from levelDb
		if err := setRLimit(1024); err != nil { // LevelDb opens many files!
			log.Printf("Error setting rlimit: %v", err)
			return
		}
		processEverythingLevelDb(*connStr, *blocksPath, *indexPath, *chainStatePath, magic, *cacheSize)
	}

}

func processEverythingBtcNode(dbconnect, addr string, tmout time.Duration, cacheSize int) {

	writer, err := db.NewPGWriter(dbconnect, cacheSize, nil)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	lastHeight, lastHash, err := writer.LastHeightHash()
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	log.Printf("Reading block headers from Node (%s)...", addr)
	bhs, err := btcnode.ReadBtcnodeBlockHeaderIndex(addr, tmout, lastHash, lastHeight)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
		return
	}
	log.Printf("Read %d block headers.", bhs.Count())
	if bhs.Count() == 0 {
		log.Fatalf("Nothing to do.")
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

	if err := processBlocks(writer, bhs, interrupt); err != nil {
		log.Printf("Error processing blocks: %v", err)
	}

	log.Printf("Closing channel, waiting for workers to finish...")
	writer.Close()
	log.Printf("All done.")
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

	lastHeight, _, err := writer.LastHeightHash()
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

	if err := processBlocks(writer, bhs, interrupt); err != nil {
		log.Printf("Error processing blocks: %v", err)
	}

	log.Printf("Closing channel, waiting for workers to finish...")
	writer.Close()
	log.Printf("All done.")
}

func processBlocks(writer *db.PGWriter, bhs blkchain.BlockHeaderIndex, interrupt chan bool) error {
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

		writer.WriteBlockInfo(bi)
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
