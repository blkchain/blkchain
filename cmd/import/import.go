package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/blkchain/blkchain"
)

func main() {

	connStr := flag.String("connstr", "host=/var/run/postgresql dbname=blocks sslmode=disable", "Db connection string")
	blocksPath := flag.String("blocks", "", "/path/to/blocks")
	indexPath := flag.String("index", "", "/path/to/blocks/index (levelDb)")
	chainStatePath := flag.String("chainstate", "", "/path/to/blocks/chainstate (levelDb UTXO set)")
	testNet := flag.Bool("testnet", false, "Use testnet magic")
	cacheSize := flag.Int("cache-size", 1024*1024*30, "Tx hashes to cache for pervout_tx_id")

	flag.Parse()

	if *blocksPath == "" {
		log.Fatalf("-blocks path required.")
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

	if err := setRLimit(1024); err != nil { // LevelDb opens many files!
		log.Printf("Error setting rlimit: %v", err)
		return
	}
	processEverything(*connStr, *blocksPath, *indexPath, *chainStatePath, magic, *cacheSize)
}

func processEverything(dbconnect, blocksPath, indexPath, chainStatePath string, magic uint32, cacheSize int) {

	log.Printf("Reading block headers from LevelDb (%s)...", indexPath)
	bhs, err := blkchain.ReadBlockHeaderIndex(indexPath, blocksPath)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
		return
	}
	log.Printf("Read %d block headers.", bhs.Count())
	if bhs.Count() == 0 {
		log.Printf("Is Core running? Stop it first, and try again. Exiting.")
		return
	}

	utxo, err := blkchain.NewChainStateChecker(chainStatePath)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}
	defer utxo.Close()

	writer, err := blkchain.NewPGWriter(dbconnect, cacheSize, utxo)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
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

	if err := processBlocks(writer, bhs, blocksPath, magic, interrupt); err != nil {
		log.Printf("Error processing blocks: %v", err)
	}

	log.Printf("Closing channel, waiting for workers to finish...")
	writer.Close()
	log.Printf("All done.")
}

func processBlocks(writer *blkchain.PGWriter, bhs *blkchain.BlockHeaderIndex, blocksPath string, magic uint32, interrupt chan bool) error {
	lastHeight, err := writer.LastHeight()
	if err != nil {
		return err
	}
	if lastHeight > 0 {
		// lastHeight is correct, pgwriter will ignore the last block
		// starting with lastHeight (as opposed to lH-1) risks
		// skipping duplicates in case of a split
		log.Printf("Starting with block height: %d", lastHeight-1)
		bhs.Start(lastHeight - 1)
	}

	for bhs.Next() {
		bh := bhs.BlockHeader()
		if bh == nil {
			log.Printf("EOF: (Nil block header at %d).", bhs.Height())
			break
		}
		b, err := bh.ReadBlock(blocksPath, magic)
		if err != nil {
			log.Printf("Error: %v", err)
			break
		}
		writer.WriteBlockInfo(&blkchain.BlockInfo{
			Block:   b,
			Height:  int(bh.Height),
			Status:  int(bh.Status),
			FileN:   int(bh.FileN),
			FilePos: int(bh.DataPos),
		})
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
