package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/blkchain/blkchain"
	"github.com/lib/pq"
)

// Explanation of how we handle integers. In Bitcoin structures most
// integers are uint32. Postgres does not have an unsigned int type,
// but using a bigint to store integers seems like a waste of
// space. So we cast all uints to int32, and thus 0xFFFFFFFF would
// become -1 in Postgres, which is fine as long as we know all the
// bits are correct.

var writerWg sync.WaitGroup

type blockRecSync struct {
	*BlockRec
	sync chan bool
}

type PGWriter struct {
	blockCh    chan *blockRecSync
	wg         *sync.WaitGroup
	db         *sql.DB
	start      time.Time
	zfsDataset string
	parallel   int
}

type isUTXOer interface {
	IsUTXO(blkchain.Uint256, uint32) (bool, error)
}

func NewPGWriter(connstr string, cacheSize int, utxo isUTXOer, zfsDataset string, parallel int) (*PGWriter, error) {

	start := time.Now()

	var (
		wg sync.WaitGroup

		firstImport = true

		db  *sql.DB
		err error
	)

	if connstr != "nulldb" {
		db, err = sql.Open("postgres", connstr)
		if err != nil {
			return nil, err
		}

		if err := createPgcrypto(db); err != nil {
			return nil, err
		}

		if err := createTables(db); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				// this is fine, cancel deferred index/constraint creation
				firstImport = false
				log.Printf("Tables already exist, catching up.")
			} else {
				return nil, err
			}
		}

		if firstImport {
			if utxo == nil {
				return nil, fmt.Errorf("First import must be done with UTXO checker, i.e. from LevelDb directly. (utxo == nil)")
			}
			log.Printf("Tables created without indexes, which are created at the very end.")
			log.Printf("Setting table parameters: autovacuum_enabled=false")
			if err := setTableStorageParams(db); err != nil {
				return nil, err
			}
		}

		if err := createPrevoutMissTable(db); err != nil {
			return nil, err
		}
	}

	bch := make(chan *blockRecSync, 2)
	wg.Add(1)

	w := &PGWriter{
		blockCh:    bch,
		wg:         &wg,
		db:         db,
		start:      start,
		zfsDataset: zfsDataset,
		parallel:   parallel,
	}

	go w.pgBlockWorker(bch, &wg, firstImport, cacheSize, utxo)

	return w, nil
}

func (p *PGWriter) Close() {
	close(p.blockCh)
	p.wg.Wait()
}

func (p *PGWriter) Uptime() time.Duration {
	return time.Now().Sub(p.start)
}

func (p *PGWriter) WriteBlock(b *BlockRec, sync bool) error {
	bs := &blockRecSync{BlockRec: b}
	if sync {
		bs.sync = make(chan bool)
	}
	p.blockCh <- bs
	if sync {
		if ok := <-bs.sync; !ok {
			log.Printf("Error writing block: %v", b.Block.Hash())
			return fmt.Errorf("Error writing block: %v", b.Block.Hash())
		}
	}
	return nil
}

func (w *PGWriter) HeightAndHashes(back int) (map[int][]blkchain.Uint256, error) {
	return getHeightAndHashes(w.db, back)
}

func (w *PGWriter) pgBlockWorker(ch <-chan *blockRecSync, wg *sync.WaitGroup, firstImport bool, cacheSize int, utxo isUTXOer) {
	defer wg.Done()

	bid, err := getLastBlockId(w.db)
	if err != nil {
		log.Printf("Error getting last block id, exiting: %v", err)
		return
	}
	txid, err := getLastTxId(w.db)
	if err != nil {
		log.Printf("Error getting last tx id, exiting: %v", err)
		return
	}

	blockCh := make(chan *blockRecSync, 2)
	go pgBlockWriter(blockCh, w.db)

	txCh := make(chan *txRec, 64)
	go pgTxWriter(txCh, w.db)

	txInCh := make(chan *txInRec, 64)
	go pgTxInWriter(txInCh, w.db, firstImport)

	txOutCh := make(chan *txOutRec, 64)
	go pgTxOutWriter(txOutCh, w.db, utxo)

	writerWg.Add(4)

	hashes, err := getHeightAndHashes(w.db, 1)
	if err != nil {
		log.Printf("Error getting last hash and height, exiting: %v", err)
		return
	}

	// nil utxo means this is coming from a btcnode, we do not need to skip blocks
	if utxo != nil && len(hashes) > 0 {
		var bhash blkchain.Uint256
		for _, hh := range hashes {
			bhash = hh[len(hh)-1] // last hash in the list is the last hash
		}
		log.Printf("PGWriter ignoring blocks up to hash %v", bhash)
		skip, last := 0, time.Now()
		for b := range ch {
			hash := b.Block.Hash()
			if bhash == hash {
				break
			} else {
				skip++
				if skip%10 == 0 && time.Now().Sub(last) > 5*time.Second {
					log.Printf(" - ignored %d blocks...", skip)
					last = time.Now()
				}
			}
		}
		if skip > 0 {
			log.Printf("Ignored %d total blocks.", skip)
		}
	}

	idCache := newTxIdCache(cacheSize)

	var syncCh chan bool
	if !firstImport {
		// no firstImport means that the constraints already
		// exist, and we need to wait for a tx to be commited before
		// ins/outs can be inserted. Same with block/tx.
		syncCh = make(chan bool)

		// The cache must be warmed up in (a rare) case when we encounter a chain split
		// soon after we start to avoid duplicate txid key errors
		nBlocks := 6
		log.Printf("Warming up the txid cache going back %d blocks...", nBlocks)
		if err := warmupCache(w.db, idCache, nBlocks); err != nil {
			log.Printf("Error warming up the idCache: %v", err)
		}
		log.Printf("Warming up the cache done.")
	}

	txcnt, start, lastStatus, lastCacheStatus, lastHeight := 0, time.Now(), time.Now(), 0, -1
	blkCnt, blkSz, batchSz := 0, 0, 0
	for br := range ch {
		bid++
		blkCnt++

		br.Id = bid
		br.Hash = br.Block.Hash()

		if br.Height < 0 { // We have to look it up

			hashes, err := getHeightAndHashes(w.db, 5)
			if err != nil {
				log.Printf("pgBlockWorker() error: %v", err)
			}

		hloop:
			for height, hh := range hashes {
				for _, h := range hh {
					if h == br.PrevHash {
						br.Height = height + 1
						break hloop
					}
				}
			}

			if br.Height < 0 {
				log.Printf("pgBlockWorker: Could not connect block to a previous block on our chain, ignoring it.")
				if br.sync != nil {
					br.sync <- false
				}
				continue
			}
		}
		lastHeight = br.Height

		blkSz += br.Size()
		batchSz += br.Size()
		blockCh <- br

		for n, tx := range br.Txs {
			txid++
			txcnt++

			hash := tx.Hash()

			// Check if recently seen and add to cache.
			recentId := idCache.add(hash, txid, len(tx.TxOuts))
			txCh <- &txRec{
				id:      recentId,
				n:       n,
				blockId: bid,
				tx:      tx,
				hash:    hash,
				dupe:    recentId != txid, // dupes are not written but referred to in block_txs
			}

			if recentId != txid {
				// This is a recent transaction, nothing to do
				continue
			}

			for n, txin := range tx.TxIns {
				txInCh <- &txInRec{
					txId:    txid,
					n:       n,
					txIn:    txin,
					idCache: idCache,
				}
			}

			for n, txout := range tx.TxOuts {
				txOutCh <- &txOutRec{
					txId:  txid,
					n:     n,
					txOut: txout,
					hash:  hash,
				}
			}
		}

		if !firstImport {
			// commit after every block
			blockCh <- &blockRecSync{
				sync: syncCh,
			}
			if syncCh != nil {
				<-syncCh
			}
			txCh <- &txRec{
				sync: syncCh,
			}
			if syncCh != nil {
				<-syncCh
			}
			// NB: Outputs must be commited before inputs!
			txOutCh <- &txOutRec{
				sync: syncCh,
			}
			if syncCh != nil {
				<-syncCh
			}
			if br.sync != nil {
				// wait for it to finish
				txInCh <- &txInRec{
					sync: syncCh,
				}
				if syncCh != nil {
					<-syncCh
					br.sync <- true
				}
			} else {
				// we don't care when it finishes
				txInCh <- nil
			}
		} else if bid%1024 == 0 {
			// commit every N blocks
			if batchSz > 100_000_000 { // but not less than a specific batch size
				batchSz = 0
				blockCh <- nil
				txCh <- nil
				txInCh <- nil
				txOutCh <- nil
			}
		}

		// report progress
		if time.Now().Sub(lastStatus) > 5*time.Second {
			uptime := w.Uptime().Round(time.Second)
			log.Printf("Height: %d Txs: %d Time: %v Tx/s: %02f KB/s: %02f Runtime: %s",
				br.Height, txcnt,
				time.Unix(int64(br.Time), 0),
				float64(txcnt)/time.Now().Sub(start).Seconds(),
				float64(blkSz/1024)/time.Now().Sub(start).Seconds(),
				uptime)
			lastStatus = time.Now()
			lastCacheStatus++
			if lastCacheStatus == 5 {
				idCache.reportStats(false)
				lastCacheStatus = 0
			}
		}
	}

	close(blockCh)
	close(txInCh)
	close(txOutCh)
	close(txCh)

	log.Printf("Closed db channels, waiting for workers to finish...")
	writerWg.Wait()
	log.Printf("Workers finished.")

	if blkCnt == 0 {
		return
	}

	idCache.reportStats(true) // Final stats ane last time

	if w.db == nil {
		return
	}

	verbose := firstImport

	if firstImport {
		idCache.clear()
		log.Printf("Cleared the cache.")

		// First only indexes necessary for fixPrevoutTxId
		log.Printf("Creating indexes part 1, please be patient, this may take a long time...")
		if err := createIndexes1(w.db, verbose); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}

		if len(w.zfsDataset) > 0 {
			takeSnapshot(w.db, w.zfsDataset, lastHeight, "-preindex")
		}

		if idCache.miss > 0 {
			log.Printf("Running ANALYZE txins, _prevout_miss, txs to ensure the next step selects the optimal plan...")
			start := time.Now()
			if err := fixPrevoutTxIdAnalyze(w.db); err != nil {
				log.Printf("Error running ANALYZE: %v", err)
			}
			log.Printf("...done in %s. Fixing missing prevout_tx_id entries (if needed), this may take a long time..",
				time.Now().Sub(start).Round(time.Millisecond))
			start = time.Now()
			if err := fixPrevoutTxId(w.db, w.parallel, true); err != nil {
				log.Printf("Error fixing prevout_tx_id: %v", err)
			}
			log.Printf("...done in %s.", time.Now().Sub(start).Round(time.Millisecond))
		} else {
			log.Printf("NOT fixing missing prevout_tx_id entries because there were 0 cache misses.")
		}

		// Now the rest of the indexes
		log.Printf("Creating indexes part 2, please be patient, this may take a long time...")
		if err := createIndexes2(w.db, verbose); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}

		// Finally constraints
		log.Printf("Creating constraints (if needed), please be patient, this may take a long time...")
		if err := createConstraints(w.db, verbose); err != nil {
			log.Printf("Error creating constraints: %v", err)
		}

		// NOTE: It is imperative that this trigger is created *after* the fixPrevoutTxId() call, or else these
		// triggers will be needlessly triggered slowing fixPrevoutTxId() tremendously. The trigger sets the spent
		// column, which should anyway be correctly set during the initial import based on the LevelDb UTXO set.
		log.Printf("Creating txins triggers.")
		if err := createTxinsTriggers(w.db); err != nil {
			log.Printf("Error creating txins triggers: %v", err)
		}

	}

	log.Printf("Dropping _prevout_miss table.")
	if err := dropPrevoutMissTable(w.db); err != nil {
		log.Printf("Error dropping _prevout_miss table: %v", err)
	}

	orphanLimit, start := 0, time.Now()
	if !firstImport {
		// No need to walk back the entire chain
		orphanLimit = blkCnt + 50
		log.Printf("Marking orphan blocks (going back %d blocks)...", orphanLimit)
	} else {
		log.Printf("Marking orphan blocks (whole chain)...")
	}
	if err := w.SetOrphans(orphanLimit); err != nil {
		log.Printf("Error marking orphans: %v", err)
	}
	log.Printf("Done marking orphan blocks in %s.", time.Now().Sub(start).Round(time.Millisecond))

	if firstImport {
		if err := resetTableStorageParams(w.db); err != nil {
			log.Printf("Error resetting storage parameters: %v", err)
		}
		log.Printf("Reset table storage parameters: autovacuum_enabled.")
		log.Printf("Indexes and constraints created.")
		if len(w.zfsDataset) > 0 {
			takeSnapshot(w.db, w.zfsDataset, lastHeight, "-postindex")
		}
	}
}

func pgBlockWriter(c chan *blockRecSync, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "height", "hash", "version", "prevhash", "merkleroot", "time", "bits", "nonce", "orphan", "size", "base_size", "weight", "virt_size"}

	txn, stmt, err := begin(db, "blocks", cols)
	if err != nil {
		log.Printf("ERROR (1): %v", err)
	}

	for br := range c {

		if br == nil || br.BlockRec == nil { // commit signal
			if err := commit(stmt, txn, nil); err != nil {
				log.Printf("Block commit error: %v", err)
			}
			txn, stmt, err = begin(db, "blocks", cols)
			if err != nil {
				log.Printf("ERROR (2): %v", err)
			}
			if br != nil && br.sync != nil {
				br.sync <- true
			}
			continue
		}

		if stmt != nil {
			b := br.Block
			if _, err := stmt.Exec(
				br.Id,
				br.Height,
				br.Hash[:],
				int32(b.Version),
				b.PrevHash[:],
				b.HashMerkleRoot[:],
				int32(b.Time),
				int32(b.Bits),
				int32(b.Nonce),
				br.Orphan,
				br.Size(),
				br.BaseSize(),
				br.Weight(),
				br.VirtualSize(),
			); err != nil {
				log.Printf("ERROR (3): %v", err)

			}
		}
	}

	log.Printf("Block writer channel closed, commiting transaction.")
	if err = commit(stmt, txn, nil); err != nil {
		log.Printf("Block commit error: %v", err)
	}
	log.Printf("Block writer done.")
}

func pgTxWriter(c chan *txRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "txid", "version", "locktime", "size", "base_size", "weight", "virt_size"}
	bcols := []string{"block_id", "n", "tx_id"}

	txn, stmt, err := begin(db, "txs", cols)
	if err != nil {
		log.Printf("ERROR (3): %v", err)
	}

	btxn, bstmt, err := begin(db, "block_txs", bcols)
	if err != nil {
		log.Printf("ERROR (4): %v", err)
	}

	// It's not clear that parallelizing Txs provides any speed
	// advntage, but whatever.

	nWorkers := 3
	wch := make(chan *txRec, 4)
	var wg sync.WaitGroup
	worker := func() {
		wg.Add(1)
		defer wg.Done()
		for tr := range wch {

			if !tr.dupe {

				if stmt != nil {
					t := tr.tx
					if _, err := stmt.Exec(
						tr.id,
						tr.hash[:],
						int32(t.Version),
						int32(t.LockTime),
						t.Size(),
						t.BaseSize(),
						t.Weight(),
						t.VirtualSize(),
					); err != nil {
						log.Printf("ERROR (7): %v", err)
					}
				}
				// It can still be a dupe if we are catching up and the
				// cache is empty, which is why we warmupCache.
			}

			if bstmt != nil {
				if _, err := bstmt.Exec(
					tr.blockId,
					tr.n,
					tr.id,
				); err != nil {
					log.Printf("ERROR (7.5): %v", err)
				}
			}

		}
	}

	// initital start workers
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	for tr := range c {
		if tr == nil || tr.tx == nil { // commit signal
			// close channel, wait for workers to finish
			close(wch)
			wg.Wait()
			// commit
			if err := commit(stmt, txn, nil); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			if err = commit(bstmt, btxn, nil); err != nil {
				log.Printf("Block Txs commit error: %v", err)
			}
			var err error
			txn, stmt, err = begin(db, "txs", cols)
			if err != nil {
				log.Printf("ERROR (5): %v", err)
			}
			btxn, bstmt, err = begin(db, "block_txs", bcols)
			if err != nil {
				log.Printf("ERROR (6): %v", err)
			}
			if tr != nil && tr.sync != nil {
				tr.sync <- true
			}
			// make a new channel, restart workers
			wch = make(chan *txRec, 4)
			for i := 0; i < nWorkers; i++ {
				go worker()
			}
			continue
		}
		wch <- tr
	}
	close(wch)
	wg.Wait()

	log.Printf("Tx writer channel closed, committing transaction.")

	if err := commit(stmt, txn, nil); err != nil {
		log.Printf("Tx commit error: %v", err)
	}
	if err := commit(bstmt, btxn, nil); err != nil {
		log.Printf("Block Txs commit error: %v", err)
	}

	log.Printf("Tx writer done.")
}

func pgTxInWriter(c chan *txInRec, db *sql.DB, firstImport bool) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "prevout_tx_id", "prevout_n", "scriptsig", "sequence", "witness"}

	txn, stmt, err := begin(db, "txins", cols)
	if err != nil {
		log.Printf("ERROR (9): %v", err)
	}

	misses := make([]*prevoutMiss, 0, 2000)

	// Parallelizing txins provides a small speed up, especially
	// during first import when we write misses to the db directly. We
	// also have a dedicated worker for prevout misses during
	// firstImport.

	nWorkers := 8
	wch := make(chan *txInRec, 4)
	mch := make(chan *prevoutMiss, 64)
	var (
		wg sync.WaitGroup
		mg sync.WaitGroup
	)

	missWriter := func() { // prevoutMiss recorder
		mg.Add(1)
		defer mg.Done()
		for miss := range mch {
			// write it to the DB directly
			if stmt != nil {
				if err := recordPrevoutMiss(db, miss.txId, miss.n, miss.prevOutHash); err != nil {
					log.Printf("ERROR (10.7): %v", err)
				}
			}
		}
	}

	worker := func() {
		wg.Add(1)
		defer wg.Done()
		for tr := range wch {

			t := tr.txIn
			var wb interface{}
			if t.Witness != nil {
				var b bytes.Buffer
				blkchain.BinWrite(&t.Witness, &b)
				wb = b.Bytes()
			}

			var prevOutTxId *int64 = nil
			if t.PrevOut.N != 0xffffffff { // coinbase
				prevOutTxId = tr.idCache.check(t.PrevOut.Hash)

				if prevOutTxId == nil { // cache miss
					if firstImport {
						mch <- &prevoutMiss{tr.txId, tr.n, t.PrevOut.Hash}
					} else {
						// remember it, it will be written just when needed
						misses = append(misses, &prevoutMiss{tr.txId, tr.n, t.PrevOut.Hash})
					}
				}
			}

			if stmt != nil {
				if _, err := stmt.Exec(
					tr.txId,
					tr.n,
					prevOutTxId,
					int32(t.PrevOut.N),
					t.ScriptSig,
					int32(t.Sequence),
					wb,
				); err != nil {
					log.Printf("ERROR (11): %v", err)
				}
			}
		}
	}

	// initital start workers
	go missWriter()
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	for tr := range c {
		if tr == nil || tr.txIn == nil { // commit signal
			// close channel, wait for workers to finish
			close(wch)
			wg.Wait()
			close(mch)
			mg.Wait()
			// commit
			if err := commit(stmt, txn, misses); err != nil {
				log.Printf("Txin commit error: %v", err)
			}
			misses = misses[:0]
			var err error
			txn, stmt, err = begin(db, "txins", cols)
			if err != nil {
				log.Printf("ERROR (10): %v", err)
			}
			if tr != nil && tr.sync != nil {
				tr.sync <- true
			}
			// make a new channel, restart workers
			mch = make(chan *prevoutMiss, 64)
			go missWriter()
			wch = make(chan *txInRec, 4)
			for i := 0; i < nWorkers; i++ {
				go worker()
			}
			continue
		}
		wch <- tr
	}
	close(wch)
	wg.Wait()
	close(mch)
	mg.Wait()

	log.Printf("TxIn writer channel closed, committing transaction.")
	if err := commit(stmt, txn, misses); err != nil {
		log.Printf("TxIn commit error: %v", err)
	}
	log.Printf("TxIn writer done.")
}

func pgTxOutWriter(c chan *txOutRec, db *sql.DB, utxo isUTXOer) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "value", "scriptpubkey", "spent"}

	txn, stmt, err := begin(db, "txouts", cols)
	if err != nil {
		log.Printf("ERROR (12): %v", err)
	}

	// The call to IsUTXO() is very time consuming because it involves
	// LevelDb, parallelizing here provides a huge speed up.
	nWorkers := 8
	wch := make(chan *txOutRec, 4)
	var wg sync.WaitGroup
	worker := func() {
		wg.Add(1)
		defer wg.Done()
		for tr := range wch {
			var spent bool
			if utxo != nil {
				// NB: Some early unspent coins are not in the UTXO set,
				// probably because they are not spendable? So the spent
				// flag could also be interpeted as unspendable.
				isUTXO, err := utxo.IsUTXO(tr.hash, uint32(tr.n))
				if err != nil {
					log.Printf("ERROR (13.5): %v", err)
				}
				spent = !isUTXO
			}
			if stmt != nil {
				t := tr.txOut
				if _, err := stmt.Exec(
					tr.txId,
					tr.n,
					t.Value,
					t.ScriptPubKey,
					spent,
				); err != nil {
					log.Printf("ERROR (13.6): %v\n", err)
				}
			}
		}
	}

	// initital start workers
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	for tr := range c {
		if tr == nil || tr.txOut == nil { // commit signal
			// close channel, wait for workers to finish
			close(wch)
			wg.Wait()
			// commit
			if err := commit(stmt, txn, nil); err != nil {
				log.Printf("TxOut commit error: %v", err)
			}
			var err error
			txn, stmt, err = begin(db, "txouts", cols)
			if err != nil {
				log.Printf("ERROR (13): %v", err)
			}
			if tr != nil && tr.sync != nil {
				tr.sync <- true
			}
			// make a new channel, restart workers
			wch = make(chan *txOutRec, 4)
			for i := 0; i < nWorkers; i++ {
				go worker()
			}
			continue
		}
		wch <- tr
	}
	close(wch)
	wg.Wait()

	log.Printf("TxOut writer channel closed, committing transaction.")
	if err := commit(stmt, txn, nil); err != nil {
		log.Printf("TxOut commit error: %v", err)
	}
	log.Printf("TxOut writer done.")
}

func begin(db *sql.DB, table string, cols []string) (*sql.Tx, *sql.Stmt, error) {
	if db == nil {
		return nil, nil, nil
	}

	txn, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	if _, err := txn.Exec("SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return nil, nil, err
	}
	stmt, err := txn.Prepare(pq.CopyIn(table, cols...))
	if err != nil {
		return nil, nil, err
	}
	return txn, stmt, nil
}

func commit(stmt *sql.Stmt, txn *sql.Tx, misses []*prevoutMiss) (err error) {
	if stmt == nil {
		return nil
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	// We do this here because it is not possible to use Exec() during
	// a pq.CopyIn operation, but we do want this done prior to the
	// Commit. Note that misses is always empty during first import,
	// which writes misses to the db directly, bypassing misses slice.
	if len(misses) > 0 {
		if err = recordPrevoutMisses(txn, misses); err != nil {
			return err
		}
		// TODO: Make the parallel argument below configurable?
		if err = fixPrevoutTxId(txn, 4, false); err != nil { // also truncates _prevout_miss
			return err
		}
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func getHeightAndHashes(db *sql.DB, back int) (map[int][]blkchain.Uint256, error) {
	if db == nil {
		return nil, nil
	}

	stmt := `SELECT height, hash FROM blocks
		WHERE height > (SELECT MAX(height) FROM blocks) - $1`

	rows, err := db.Query(stmt, back)
	if err != nil {
		return nil, fmt.Errorf("getHeightAndHashes: %v", err)
	}
	defer rows.Close()

	hashes := make(map[int][]blkchain.Uint256)

	for rows.Next() {
		var (
			height int
			hash   blkchain.Uint256
		)
		if err := rows.Scan(&height, &hash); err != nil {
			return nil, err
		}
		if list, ok := hashes[height]; ok {
			hashes[height] = append(list, hash)
		} else {
			hashes[height] = []blkchain.Uint256{hash}
		}
	}
	if len(hashes) > 0 {
		return hashes, nil
	}
	return nil, rows.Err()
}

func getLastBlockId(db *sql.DB) (int, error) {
	if db == nil {
		return 0, nil
	}

	rows, err := db.Query("SELECT id FROM blocks ORDER BY id DESC LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, rows.Err()
}

func getLastTxId(db *sql.DB) (int64, error) {
	if db == nil {
		return 0, nil
	}

	rows, err := db.Query("SELECT id FROM txs ORDER BY id DESC LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, rows.Err()
}

// This could be a db connection or a transaction
type execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
}

func createPrevoutMissTable(db execer) error {
	sql := `
     CREATE UNLOGGED TABLE IF NOT EXISTS _prevout_miss (
          id            SERIAL NOT NULL PRIMARY KEY
         ,tx_id         BIGINT NOT NULL
         ,n             INT NOT NULL
         ,prevout_hash  BYTEA NOT NULL
     );
     TRUNCATE _prevout_miss RESTART IDENTITY;
`
	_, err := db.Exec(sql)
	return err
}

func clearPrevoutMissTable(db execer) error {
	_, err := db.Exec("TRUNCATE _prevout_miss RESTART IDENTITY")
	return err
}

func dropPrevoutMissTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS _prevout_miss")
	return err
}

func prevoutMissMaxId(db execer) (int, error) {
	stmt := `SELECT MAX(id) FROM _prevout_miss`
	rows, err := db.Query(stmt)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var max int
	for rows.Next() {
		if err := rows.Scan(&max); err != nil {
			return 0, err
		}
	}
	return max, nil
}

func recordPrevoutMiss(db *sql.DB, txId int64, n int, prevoutHash blkchain.Uint256) error {
	stmt := "INSERT INTO _prevout_miss (tx_id, n, prevout_hash) VALUES($1, $2, $3)"
	_, err := db.Exec(stmt, txId, n, prevoutHash[:])
	if err != nil {
		return err
	}
	return nil
}

type prevoutMiss struct {
	txId        int64
	n           int
	prevOutHash blkchain.Uint256
}

func recordPrevoutMisses(txn *sql.Tx, misses []*prevoutMiss) error {
	stmt, err := txn.Prepare(pq.CopyIn("_prevout_miss", "tx_id", "n", "prevout_hash"))
	if err != nil {
		return err
	}
	for i, m := range misses {
		if _, err = stmt.Exec(m.txId, m.n, m.prevOutHash[:]); err != nil {
			return err
		}
		misses[i] = nil // so that they can be freed
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	return nil
}

// Most of the prevout_tx_id's should be already set during the
// import, but we need to correct the remaining ones. You can likely
// avoid it by increasing the txIdCache size. After the first import
// it will be looked up on the fly as needed, but a larger cache is
// still important. We run it in parallel to speed things up.
func fixPrevoutTxId(db execer, parallel int, verbose bool) error {
	if parallel == 0 {
		parallel = 1
	}
	stmt := `
UPDATE txins i
   SET prevout_tx_id = t.id
  FROM _prevout_miss m
  JOIN txs t ON m.prevout_hash = t.txid
 WHERE m.id >= $1 AND m.id < $2
   AND i.tx_id = m.tx_id
   AND i.n = m.n`

	max, err := prevoutMissMaxId(db)
	if err != nil {
		return err
	}
	if verbose {
		log.Printf("  max prevoutMiss id: %d parallel: %d", max, parallel)
	}

	if max == 0 {
		return nil // nothing to do
	}

	step := max / (parallel - 1) // -1 because we do not one left over
	if step > 10000 {
		step = 10000 // kinda arbitrary - the idea is to show progress
	}
	if step < 10 {
		step = 10
	}

	// Start workers
	type startEnd struct{ start, end int }
	queue := make(chan startEnd)
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		go func(queue chan startEnd) {
			wg.Add(1)
			defer wg.Done()

			for se := range queue {
				if verbose {
					log.Printf("  processing range [%d, %d) of %d...", se.start, se.end, max)
				}
				if _, err := db.Exec(stmt, se.start, se.end); err != nil {
					log.Printf(" range [%d, %d) error: %v", se.start, se.end, err)
				}
			}
		}(queue)
	}

	for i := 1; i <= max; i += step {
		queue <- startEnd{i, i + step}
	}
	close(queue)
	wg.Wait()

	return clearPrevoutMissTable(db)
}

func fixPrevoutTxIdAnalyze(db execer) error {
	_, err := db.Exec(`ANALYZE txins, _prevout_miss, txs`)
	return err
}

func createTables(db *sql.DB) error {
	sqlTables := `
  CREATE TABLE blocks (
   id           INT NOT NULL
  ,height       INT NOT NULL -- not same as id, because orphans.
  ,hash         BYTEA NOT NULL
  ,version      INT NOT NULL
  ,prevhash     BYTEA NOT NULL
  ,merkleroot   BYTEA NOT NULL
  ,time         INT NOT NULL
  ,bits         INT NOT NULL
  ,nonce        INT NOT NULL
  ,orphan       BOOLEAN NOT NULL DEFAULT false
  ,size         INT NOT NULL
  ,base_size    INT NOT NULL
  ,weight       INT NOT NULL
  ,virt_size    INT NOT NULL
  );

  CREATE TABLE txs (
   id            BIGINT NOT NULL
  ,txid          BYTEA NOT NULL
  ,version       INT NOT NULL
  ,locktime      INT NOT NULL
  ,size          INT NOT NULL
  ,base_size     INT NOT NULL
  ,weight        INT NOT NULL
  ,virt_size     INT NOT NULL
  );

  CREATE TABLE block_txs (
   block_id      INT NOT NULL
  ,n             SMALLINT NOT NULL
  ,tx_id         BIGINT NOT NULL
  );

  CREATE TABLE txins (
   tx_id         BIGINT NOT NULL
  ,n             SMALLINT NOT NULL
  ,prevout_tx_id BIGINT   -- can be NULL for coinbase
  ,prevout_n     SMALLINT NOT NULL
  ,scriptsig     BYTEA NOT NULL
  ,sequence      INT NOT NULL
  ,witness       BYTEA
  );

  CREATE TABLE txouts (
   tx_id        BIGINT NOT NULL
  ,n            SMALLINT NOT NULL
  ,value        BIGINT NOT NULL
  ,scriptpubkey BYTEA NOT NULL
  ,spent        BOOL NOT NULL
  );
`
	_, err := db.Exec(sqlTables)
	return err
}

// The approach we are taking here is to disable autovacuum during the
// first import, then enabling it at the end and let it run. Because of hint
// bits, the vacuum process will effectively rewrite every page of the table, this is explained here
// https://dba.stackexchange.com/questions/130496/is-it-worth-it-to-run-vacuum-on-a-table-that-only-receives-inserts
// NB: Setting UNLOGGED/LOGGED does not work as changing this setting rewrites the tables
func setTableStorageParams(db *sql.DB) error {
	for _, table := range []string{"blocks", "txs", "block_txs", "txins", "txouts"} {
		stmt := fmt.Sprintf(`ALTER TABLE %s SET (autovacuum_enabled=false)`, table)
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
func resetTableStorageParams(db *sql.DB) error {
	for _, table := range []string{"blocks", "txs", "block_txs", "txins", "txouts"} {
		stmt := fmt.Sprintf(`ALTER TABLE %s RESET (autovacuum_enabled)`, table)
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func createPgcrypto(db *sql.DB) error {
	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto")
	return err
}

// Create indexes only necessary to run fixPrevoutTxId(). Since we are
// updating txins, a presense of the txins(addr_prefix(... index would
// slow the updates down a great deal (updates cause an index update,
// which calls the function).
func createIndexes1(db *sql.DB, verbose bool) error {
	var start time.Time
	if verbose {
		log.Printf("  Starting txins primary key...")
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txins' AND constraint_name = 'txins_pkey') THEN
            ALTER TABLE txins ADD CONSTRAINT txins_pkey PRIMARY KEY(tx_id, n);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txs txid (hash) index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS txs_txid_idx ON txs(txid);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s.", time.Now().Sub(start).Round(time.Millisecond))
	}
	return nil
}

func createIndexes2(db *sql.DB, verbose bool) error {
	var start time.Time
	// Adding a constraint or index if it does not exist is a little tricky in PG
	if verbose {
		log.Printf("  Starting blocks primary key...")
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'blocks' AND constraint_name = 'blocks_pkey') THEN
            ALTER TABLE blocks ADD CONSTRAINT blocks_pkey PRIMARY KEY(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting blocks prevhash index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS blocks_prevhash_idx ON blocks(prevhash);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting blocks hash index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS blocks_hash_idx ON blocks(hash);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting blocks height index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS blocks_height_idx ON blocks(height);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txs primary key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txs_pkey') THEN
            ALTER TABLE txs ADD CONSTRAINT txs_pkey PRIMARY KEY(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting block_txs block_id, n primary key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'block_txs' AND constraint_name = 'block_txs_pkey') THEN
            ALTER TABLE block_txs ADD CONSTRAINT block_txs_pkey PRIMARY KEY(block_id, n);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting block_txs tx_id index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS block_txs_tx_id_idx ON block_txs(tx_id);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Creatng hash_type function...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       CREATE OR REPLACE FUNCTION hash_type(_hash BYTEA) RETURNS TEXT AS $$
       BEGIN
         IF EXISTS (SELECT 1 FROM blocks WHERE hash = _hash) THEN
           RETURN 'block';
         ELSIF EXISTS (SELECT 1 FROM txs WHERE txid = _hash) THEN
           RETURN 'tx';
         END IF;
         RETURN NULL;
       END;
       $$ LANGUAGE plpgsql;
    `); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txins (prevout_tx_id, prevout_tx_n) index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS txins_prevout_tx_id_prevout_n_idx ON txins(prevout_tx_id, prevout_n);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txouts primary key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txouts' AND constraint_name = 'txouts_pkey') THEN
            ALTER TABLE txouts ADD CONSTRAINT txouts_pkey PRIMARY KEY(tx_id, n);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txouts address prefix index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
           CREATE OR REPLACE FUNCTION extract_address(scriptPubKey BYTEA) RETURNS BYTEA AS $$
           BEGIN
             IF SUBSTR(scriptPubKey, 1, 3) = E'\\x76a914' THEN  -- P2PKH
               RETURN SUBSTR(scriptPubKey, 4, 20);
             ELSIF SUBSTR(scriptPubKey, 1, 2) = E'\\xa914' THEN -- P2SH
               RETURN SUBSTR(scriptPubKey, 3, 20);
             ELSIF SUBSTR(scriptPubKey, 1, 2) = E'\\x0014' THEN -- P2WPKH
               RETURN SUBSTR(scriptPubKey, 3, 20);
             ELSIF SUBSTR(scriptPubKey, 1, 2) = E'\\x0020' THEN -- P2WSH
               RETURN SUBSTR(scriptPubKey, 3, 32);
             END IF;
             RETURN NULL;
           END;
           $$ LANGUAGE plpgsql IMMUTABLE;

           -- Cast the first 8 bytes of a BYTEA as a BIGINT
           CREATE OR REPLACE FUNCTION bytes2int8(bytes BYTEA) RETURNS BIGINT AS $$
           BEGIN
             RETURN SUBSTR(bytes::text, 2, 16)::bit(64)::bigint;
           END;
           $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

           -- Address prefix (txout)
           CREATE OR REPLACE FUNCTION addr_prefix(scriptPubKey BYTEA) RETURNS BIGINT AS $$
           BEGIN
             RETURN public.bytes2int8(public.extract_address(scriptPubKey));
           END;
           $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

           CREATE INDEX IF NOT EXISTS txouts_addr_prefix_tx_id_idx ON txouts(addr_prefix(scriptpubkey), tx_id);
       `); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txins address prefix index...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
        CREATE OR REPLACE FUNCTION parse_witness(witness BYTEA) RETURNS BYTEA[] AS $$
        DECLARE
          stack BYTEA[];
          len INT;
          pos INT = 1;
          slen INT;
        BEGIN
          IF witness IS NULL OR witness = '' THEN
            RETURN NULL;
          END IF;
          len = GET_BYTE(witness, 0); -- this is a varint, but whatever
          WHILE len > 0 LOOP
            slen = GET_BYTE(witness, pos);
            IF slen = 253 THEN
              slen = GET_BYTE(witness, pos+1) + GET_BYTE(witness, pos+2)*256;
              pos = pos+2;
              -- NB: There is a possibility of a 4-byte compact, but no transaction uses it
            END IF;
            stack = stack || SUBSTR(witness, pos+2, slen);
            pos = pos + slen + 1;
            len = len - 1;
          END LOOP;
          RETURN stack;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

        CREATE OR REPLACE FUNCTION extract_address(scriptsig BYTEA, witness BYTEA) RETURNS BYTEA AS $$
        DECLARE
          pub BYTEA;
          sha BYTEA;
          wits BYTEA[];
          len INT;
          pos INT;
          op INT;
        BEGIN
          IF LENGTH(scriptsig) = 0 OR scriptsig IS NULL THEN     -- Native SegWit: P2WSH or P2WPKH
            wits = public.parse_witness(witness);
            pub = wits[array_length(wits, 1)];
            sha = public.digest(pub, 'sha256');
            IF ARRAY_LENGTH(wits, 1) = 2 AND LENGTH(pub) = 33 THEN       -- Most likely a P2WPKH
              RETURN public.digest(sha, 'ripemd160');
            ELSE       -- Most likely a P2WSH
              RETURN sha;
            END IF;
          ELSE
             len = GET_BYTE(scriptsig, 0);
             IF len = LENGTH(scriptsig) - 1 THEN        -- Most likely a P2SH (or P2SH-P2W*)
               RETURN public.digest(public.digest(SUBSTR(scriptsig, 2), 'sha256'), 'ripemd160');
             ELSE  -- P2PKH or longer P2SH, either way the last thing is what we need
               pos = 0;
               WHILE pos < LENGTH(scriptsig)-1 LOOP
                 op = GET_BYTE(scriptsig, pos);
                 IF op > 0 AND op < 76 THEN
                   len = op;
                   pos = pos + 1;
                 ELSEIF op = 76 THEN
                   len = GET_BYTE(scriptsig, pos+1);
                   pos = pos + 2;
                 ELSEIF op = 77 THEN
                   len = GET_BYTE(scriptsig, pos+1) + GET_BYTE(scriptsig, pos+2)*256;
                   pos = pos + 3;
                 ELSE
                   pos = pos + 1;
                   CONTINUE;
                 END IF;
                 pub = SUBSTR(scriptsig, pos+1, len);
                 pos = pos + len;
               END LOOP;
               RETURN public.digest(public.digest(pub, 'sha256'), 'ripemd160');
             END IF;
          END IF;
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

        -- Address prefix (txin)
        CREATE OR REPLACE FUNCTION addr_prefix(scriptsig BYTEA, witness BYTEA) RETURNS BIGINT AS $$
        BEGIN
          RETURN public.bytes2int8(public.extract_address(scriptsig, witness));
        END;
        $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

        -- Partial/conditional index because coinbase txin scriptsigs are garbage
        CREATE INDEX IF NOT EXISTS txins_addr_prefix_tx_id_idx ON txins(addr_prefix(scriptsig, witness), tx_id)
         WHERE prevout_tx_id IS NOT NULL;
       `); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s.", time.Now().Sub(start).Round(time.Millisecond))
	}
	return nil
}

func createConstraints(db *sql.DB, verbose bool) error {
	var start time.Time
	if verbose {
		log.Printf("  Starting block_txs block_id foreign key...")
	}
	start = time.Now()
	if _, err := db.Exec(`
	   DO $$
	   BEGIN
	     -- NB: table_name is the target/foreign table
	     IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
	                     WHERE table_name = 'blocks' AND constraint_name = 'block_txs_block_id_fkey') THEN
	       ALTER TABLE block_txs ADD CONSTRAINT block_txs_block_id_fkey FOREIGN KEY (block_id) REFERENCES blocks(id) ON DELETE CASCADE NOT VALID;
	     END IF;
	   END
	   $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting block_txs tx_id foreign key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
	   DO $$
	   BEGIN
	     -- NB: table_name is the target/foreign table
	     IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
	                     WHERE table_name = 'txs' AND constraint_name = 'block_txs_tx_id_fkey') THEN
	       ALTER TABLE block_txs ADD CONSTRAINT block_txs_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id) ON DELETE CASCADE NOT VALID DEFERRABLE;
	     END IF;
	   END
	   $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txins tx_id foreign key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         -- NB: table_name is the target/foreign table
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txins_tx_id_fkey') THEN
           ALTER TABLE txins ADD CONSTRAINT txins_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id) ON DELETE CASCADE NOT VALID DEFERRABLE;
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s. Starting txouts tx_id foreign key...", time.Now().Sub(start).Round(time.Millisecond))
	}
	start = time.Now()
	if _, err := db.Exec(`
       DO $$
       BEGIN
         -- NB: table_name is the target/foreign table
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txouts_tx_id_fkey') THEN
           ALTER TABLE txouts ADD CONSTRAINT txouts_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id) ON DELETE CASCADE NOT VALID DEFERRABLE;
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  ...done in %s.", time.Now().Sub(start).Round(time.Millisecond))
	}
	return nil
}

// Set the orphan status starting from the highest block and going
// backwards, up to limit. If limit is 0, the whole table is updated.
//
// The WITH RECURSIVE part connects rows by joining prevhash to hash,
// thereby building a list which starts at the highest hight and going
// towards the beginning until no parent can be found.
//
// Then we LEFT JOIN the above to the blocks table, and where there is
// no match (x.id IS NULL) we mark it as orphan.
//
// If the chain is split, i.e. there is more than one row at the
// highest height, then no blocks in the split will be marked as
// orphan, which is fine.

func (w *PGWriter) SetOrphans(limit int) error {
	var limitNSql string
	if limit > 0 {
		limitNSql = fmt.Sprintf("WHERE n < %d", limit+50)
	}
	if _, err := w.db.Exec(fmt.Sprintf(`
DO $$
DECLARE
  min_id INT;
BEGIN
-- select an id going back limit rows.
SELECT MIN(id) INTO min_id FROM (SELECT id FROM blocks ORDER BY id DESC LIMIT %d) x;
-- a limit of 0 leaves min_id as NULL, need to correct that
IF (min_id IS NULL) THEN
  min_id = -1;
END IF;
-- the key idea is that the recursive part goes back further than the
-- limit imposed by min_id
UPDATE blocks
   SET orphan = a.orphan
  FROM (
    SELECT blocks.id, x.id IS NULL AS orphan
      FROM blocks
      LEFT JOIN (
        WITH RECURSIVE recur(id, prevhash, n) AS (
          -- non-recursive term, executed once
          SELECT id, prevhash, 0 AS n
            FROM blocks
                            -- this should be faster than MAX(height)
           WHERE height IN (SELECT height FROM blocks ORDER BY height DESC LIMIT 1)
          UNION ALL
          -- recursive term, recur refers to previous iteration result
          -- iteration stops when previous row prevhash finds no match OR
          -- if n reaches a limit (see limitSql above)
            SELECT blocks.id, blocks.prevhash, n+1 AS n
              FROM recur
              JOIN blocks ON blocks.hash = recur.prevhash
            %s
        )
        SELECT recur.id, recur.prevhash
          FROM recur
      ) x ON blocks.id = x.id
   ) a
  WHERE blocks.id = a.id AND blocks.id >= min_id;
END
 $$`, limit, limitNSql)); err != nil {
		return err
	}
	return nil
}

func createTxinsTriggers(db *sql.DB) error {
	if _, err := db.Exec(`

CREATE OR REPLACE FUNCTION txins_after_trigger_func() RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      IF OLD.prevout_tx_id IS NOT NULL THEN
        UPDATE txouts SET spent = FALSE
         WHERE tx_id = OLD.prevout_tx_id AND n = OLD.prevout_n;
      END IF;
      RETURN OLD;
    ELSIF (TG_OP = 'UPDATE' OR TG_OP = 'INSERT') THEN
      IF NEW.prevout_tx_id IS NOT NULL THEN
        UPDATE txouts SET spent = TRUE
         WHERE tx_id = NEW.prevout_tx_id AND n = NEW.prevout_n;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'Unknown tx_id:prevout_n combination: %:%', NEW.prevout_tx_id, NEW.prevout_n;
        END IF;
      END IF;
      RETURN NEW;
    END IF;
    RETURN NULL;
  END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

CREATE CONSTRAINT TRIGGER txins_after_trigger
AFTER INSERT OR UPDATE OR DELETE ON txins DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE PROCEDURE txins_after_trigger_func();

        `); err != nil {
		return err
	}
	return nil
}

func warmupCache(db *sql.DB, cache *txIdCache, blocks int) error {
	stmt := `
SELECT t.id, t.txid, o.cnt
  FROM txs t
  JOIN LATERAL (
    SELECT COUNT(1) AS cnt
      FROM txouts o
      WHERE o.tx_id = t.id
 ) o ON true
 WHERE id > (
    SELECT MIN(tx_id) FROM block_txs bt
      JOIN (
        SELECT id FROM blocks ORDER BY id DESC LIMIT $1
      ) b ON b.id = bt.block_id
    )
ORDER BY t.id;
`
	rows, err := db.Query(stmt, blocks)
	if err != nil {
		return err
	}
	defer rows.Close()
	var (
		txid int64
		hash blkchain.Uint256
		cnt  int
	)
	for rows.Next() {
		if err := rows.Scan(&txid, &hash, &cnt); err != nil {
			return err
		}
		cache.add(hash, txid, cnt)
	}
	return nil
}

func takeSnapshot(db *sql.DB, dataset string, height int, tag string) error {
	log.Printf("Taking a ZFS snapshot: %s@%d%s", dataset, height, tag)
	log.Printf("  calling pg_backup_start('blkchain', true)") // pg < 15 use pg_start_backup
	if _, err := db.Exec(`SELECT pg_backupstart('blkchain', true)`); err != nil {
		return err
	}
	log.Printf("  executing COPY (SELECT) TO PROGRAM 'zfs snapshot %s@%d%s'", dataset, height, tag)
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT) TO PROGRAM 'zfs snapshot %s@%d%s'`, dataset, height, tag)); err != nil {
		return err
	}
	log.Printf("  calling pg_backup_stop()") // pg < 15 use pg_stop_backup
	if _, err := db.Exec(`SELECT pg_backup_stop()`); err != nil {
		return err
	}
	return nil
}

// TODO: Presently unused
func waitForAutovacuum(db *sql.DB) error {
	avCount := func() (int, error) {
		stmt := `SELECT COUNT(1) FROM pg_stat_activity WHERE query LIKE 'autovacuum:%'`
		rows, err := db.Query(stmt)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		var cnt int
		for rows.Next() {
			if err := rows.Scan(&cnt); err != nil {
				return 0, err
			}
		}
		return cnt, nil
	}
	for {
		if cnt, err := avCount(); err != nil {
			log.Printf("waitForAutovacuum: %v", err)
			return err
		} else if cnt == 0 {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
}
