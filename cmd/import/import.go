package main

import (
	"flag"
	"io"
	"log"

	"github.com/grisha/blkchain"
)

func readBlocks(dbconnect, path string, magic uint32, startIdx int) {

	out, wg, err := blkchain.NewPGWriter(dbconnect)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	rdr, err := blkchain.NewCoreStore(path, startIdx, magic)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

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

	readBlocks(*connStr, *blocksPath, magic, *startIdx)
}
