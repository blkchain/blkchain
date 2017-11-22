package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/blkchain/blkchain"
)

func readBlocks(dbconnect, path string, magic uint32, startIdx int, cacheSize int) {

	out, wg, err := blkchain.NewPGWriter(dbconnect, cacheSize)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	rdr, err := blkchain.NewCoreStore(path, startIdx, magic)
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

	for {
		b := blkchain.Block{Magic: magic}
		err := blkchain.BinRead(&b, rdr)
		if err != nil {
			if err == io.EOF {
				log.Printf("End of files reached.")
			} else {
				log.Printf("ERROR: %v", err)
			}
			break
		}

		out <- &b

		if interrupt {
			break
		}
	}

	close(out)
	log.Printf("Closed channel, waiting for workers to finish...")
	wg.Wait()
	log.Printf("All done.")
}

func main() {

	connStr := flag.String("connstr", "host=/var/run/postgresql dbname=blocks sslmode=disable", "Db connection string")
	blocksPath := flag.String("blocks", "", "/path/to/blocks")
	testNet := flag.Bool("testnet", false, "Use testnet magic")
	startIdx := flag.Int("start-idx", 0, "Start number of the blkXXXXXX file")
	cacheSize := flag.Int("cache-size", 1024*1024*10, "Tx hashes to cache for pervout_tx_id")

	flag.Parse()

	if *blocksPath == "" {
		log.Fatalf("-blocks path required.")
	}

	var magic uint32
	if *testNet {
		magic = blkchain.TestNetMagic
	} else {
		magic = blkchain.MainNetMagic
	}

	readBlocks(*connStr, *blocksPath, magic, *startIdx, *cacheSize)
}
