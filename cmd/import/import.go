package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/blkchain/blkchain"
)

func main() {

	connStr := flag.String("connstr", "host=/var/run/postgresql dbname=blocks sslmode=disable", "Db connection string")
	blocksPath := flag.String("blocks", "", "/path/to/blocks")
	indexPath := flag.String("index", "", "/path/to/blocks/index (levelDb)")
	testNet := flag.Bool("testnet", false, "Use testnet magic")
	cacheSize := flag.Int("cache-size", 1024*1024*10, "Tx hashes to cache for pervout_tx_id")

	flag.Parse()

	if *blocksPath == "" {
		log.Fatalf("-blocks path required.")
	}

	if *indexPath == "" {
		*indexPath = filepath.Join(*blocksPath, "index")
	}

	var magic uint32
	if *testNet {
		magic = blkchain.TestNetMagic
	} else {
		magic = blkchain.MainNetMagic
	}

	readBlocks(*connStr, *blocksPath, *indexPath, magic, *cacheSize)
}

func readBlocks(dbconnect, blocksPath, indexPath string, magic uint32, cacheSize int) {

	log.Printf("Reading block headers from LevelDb (%s)...", indexPath)
	bhs, err := blkchain.ReadBlockHeaderIndex(indexPath)
	log.Printf("Read %d block headers.", len(bhs))
	if len(bhs) == 0 {
		log.Printf("Is Core running? Stop it first, and try again. Exiting.")
		return
	}

	out, wg, err := blkchain.NewPGWriter(dbconnect, cacheSize)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	interrupt := false
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Printf("Interrupt, exiting scan loop...")
		signal.Stop(sigCh)
		interrupt = true
	}()

	for height := 0; height < len(bhs); height++ {
		for _, bh := range bhs[height] {

			path := filepath.Join(blocksPath, fmt.Sprintf("%s%05d.dat", "blk", int(bh.FileN)))
			f, err := os.Open(path)
			if err != nil {
				log.Printf("Error opening file %v: %v", path, err)
				continue
			}

			pos := int64(bh.DataPos) - 8 // magic + size = 8
			_, err = f.Seek(pos, 0)
			if err != nil {
				log.Printf("Error seeking to pos %d in file %v: %v", pos, path, err)
				f.Close()
				continue
			}

			b := blkchain.Block{Magic: magic}
			err = blkchain.BinRead(&b, f)
			f.Close()

			if err != nil {
				log.Printf("Error reading block: %v", err)
				continue
			}

			out <- &blkchain.BlockInfo{
				Block:   &b,
				Height:  int(bh.Height),
				Status:  int(bh.Status),
				FileN:   int(bh.FileN),
				FilePos: int(bh.DataPos),
			}
		}
		if interrupt {
			break
		}
	}

	close(out)
	log.Printf("Closed channel, waiting for workers to finish...")
	wg.Wait()
	log.Printf("All done.")
}
