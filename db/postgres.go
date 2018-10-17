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

type blockRec struct {
	id     int
	height int
	block  *blkchain.Block
	hash   blkchain.Uint256
	orphan bool
	sync   chan bool
}

type txRec struct {
	id      int64
	blockId int
	n       int // position within block
	tx      *blkchain.Tx
	hash    blkchain.Uint256
	sync    chan bool
	dupe    bool // already seen
}

type txInRec struct {
	txId    int64
	n       int
	txIn    *blkchain.TxIn
	idCache *txIdCache
	sync    chan bool
}

type txOutRec struct {
	txId  int64
	n     int
	txOut *blkchain.TxOut
	hash  blkchain.Uint256
	sync  chan bool
}

type BlockInfo struct {
	*blkchain.Block
	Height int
	Sync   chan bool
}

type PGWriter struct {
	blockCh chan *BlockInfo
	wg      *sync.WaitGroup
	db      *sql.DB
}

type isUTXOer interface {
	IsUTXO(blkchain.Uint256, uint32) (bool, error)
}

func NewPGWriter(connstr string, cacheSize int, utxo isUTXOer) (*PGWriter, error) {

	var wg sync.WaitGroup

	db, err := sql.Open("postgres", connstr)
	if err != nil {
		return nil, err
	}

	firstImport := true // firstImport == first import
	if err := createTables(db); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// this is fine, cancel deferred index/constraint creation
			firstImport = false
		} else {
			if utxo == nil {
				return nil, fmt.Errorf("First import must be done with UTXO checker, i.e. from LevelDb directly. (utxo == nil)")
			}
			log.Printf("Tables created without indexes, which are created at the very end.")
			return nil, err
		}
	}

	bch := make(chan *BlockInfo, 2)
	wg.Add(1)

	w := &PGWriter{
		blockCh: bch,
		wg:      &wg,
		db:      db,
	}

	go w.pgBlockWorker(bch, &wg, firstImport, cacheSize, utxo)

	return w, nil
}

func (p *PGWriter) Close() {
	close(p.blockCh)
	p.wg.Wait()
}

func (p *PGWriter) WriteBlockInfo(b *BlockInfo) {
	p.blockCh <- b
}

func (w *PGWriter) HeightAndHashes() (int, []blkchain.Uint256, error) {
	return getHeightAndHashes(w.db)
}

func (w *PGWriter) pgBlockWorker(ch <-chan *BlockInfo, wg *sync.WaitGroup, firstImport bool, cacheSize int, utxo isUTXOer) {
	defer wg.Done()

	_, hashes, err := getHeightAndHashes(w.db)
	if err != nil {
		log.Printf("Error getting last hash and height, exiting: %v", err)
		return
	}
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

	blockCh := make(chan *blockRec, 2)
	go pgBlockWriter(blockCh, w.db)

	txCh := make(chan *txRec, 64)
	go pgTxWriter(txCh, w.db)

	txInCh := make(chan *txInRec, 64)
	go pgTxInWriter(txInCh, w.db)

	txOutCh := make(chan *txOutRec, 64)
	go pgTxOutWriter(txOutCh, w.db, utxo)

	writerWg.Add(4)

	start := time.Now()

	// nil utxo means this is coming from a btcnode, we do not need to skip blocks
	if utxo != nil && len(hashes) > 0 {
		bhash := hashes[len(hashes)-1] // last hash in the list is the last hash
		log.Printf("PGWriter ignoring blocks up to hash %v", bhash)
		skip, last := 0, time.Now()
		for b := range ch {
			hash := b.Hash()
			if bytes.Compare(bhash[:], hash[:]) == 0 {
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
		syncCh = make(chan bool, 0)
	}

	txcnt, last := 0, time.Now()
	blkCnt := 0
	for bi := range ch {
		bid++
		blkCnt++
		hash := bi.Hash()

		if bi.Height < 0 { // We have to look it up

			height, hashes, err := getHeightAndHashes(w.db)
			if err != nil {
				log.Printf("pgBlockWorker() error: %v", err)
			}

			for _, h := range hashes {
				if h == bi.PrevHash {
					bi.Height = height + 1
					break
				}
			}

			if bi.Height < 0 {
				log.Printf("pgBlockWorker: Could not connect block to a previous block on our chain, ignoring it.")
				continue
			}
		}

		blockCh <- &blockRec{
			id:     bid,
			height: bi.Height,
			block:  bi.Block,
			hash:   hash,
		}

		for n, tx := range bi.Txs {
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
				dupe:    recentId != txid,
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
			blockCh <- &blockRec{
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
			if bi.Sync != nil {
				// wait for it to finish
				txInCh <- &txInRec{
					sync: syncCh,
				}
				if syncCh != nil {
					<-syncCh
					bi.Sync <- true
				}
			} else {
				// we con't care when it finishes
				txInCh <- nil
			}
		} else if bid%50 == 0 {
			// commit every N blocks
			blockCh <- nil
			txCh <- nil
			txInCh <- nil
			txOutCh <- nil
		}

		// report progress
		if time.Now().Sub(last) > 5*time.Second {
			log.Printf("Height: %d Txs: %d Time: %v Tx/s: %02f",
				bi.Height, txcnt, time.Unix(int64(bi.Time), 0), float64(txcnt)/time.Now().Sub(start).Seconds())
			last = time.Now()
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

	log.Printf("Txid cache hits: %d (%.02f%%) misses: %d collisions: %d dupes: %d evictions: %d",
		idCache.hits, float64(idCache.hits)/(float64(idCache.hits+idCache.miss)+0.0001)*100,
		idCache.miss, idCache.cols, idCache.dups, idCache.evic)

	verbose := firstImport

	if firstImport {
		log.Printf("Creating indexes part 1 (if needed), please be patient, this may take a long time...")
		if err := createIndexes1(w.db, verbose); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}

		if idCache.miss > 0 {
			log.Printf("Fixing missing prevout_tx_id entries (if needed), this may take a long time...")
			if err := fixPrevoutTxId(w.db); err != nil {
				log.Printf("Error fixing prevout_tx_id: %v", err)
			}
		} else {
			log.Printf("NOT fixing missing prevout_tx_id entries because there were 0 cache misses.")

		}

		log.Printf("Creating indexes part 2 (if needed), please be patient, this may take a long time...")
		if err := createIndexes2(w.db, verbose); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}

		log.Printf("Creating constraints (if needed), please be patient, this may take a long time...")
		if err := createConstraints(w.db, verbose); err != nil {
			log.Printf("Error creating constraints: %v", err)
		}
	}

	if firstImport {
		log.Printf("Creating txins triggers.")
		if err := createTxinsTriggers(w.db); err != nil {
			log.Printf("Error creating txins triggers: %v", err)
		}
	}

	orphanLimit := 0
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
	log.Printf("Done marking orphan blocks.")

	if firstImport {
		log.Printf("Indexes and constraints created.")
	}
}

func begin(db *sql.DB, table string, cols []string) (*sql.Tx, *sql.Stmt, error) {
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

func pgBlockWriter(c chan *blockRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "height", "hash", "version", "prevhash", "merkleroot", "time", "bits", "nonce", "orphan"}

	txn, stmt, err := begin(db, "blocks", cols)
	if err != nil {
		log.Printf("ERROR (1): %v", err)
	}

	for br := range c {

		if br == nil || br.block == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
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

		b := br.block
		_, err = stmt.Exec(
			br.id,
			br.height,
			br.hash[:],
			int32(b.Version),
			b.PrevHash[:],
			b.HashMerkleRoot[:],
			int32(b.Time),
			int32(b.Bits),
			int32(b.Nonce),
			br.orphan,
		)
		if err != nil {
			log.Printf("ERROR (3): %v", err)
		}

		if br.sync != nil {
			// commit and send confirmation
			if err = commit(stmt, txn); err != nil {
				log.Printf("Block commit error (2): %v", err)
			}
			txn, stmt, err = begin(db, "blocks", cols)
			if err != nil {
				log.Printf("ERROR (2.5): %v", err)
			}
			br.sync <- true
		}

	}

	log.Printf("Block writer channel closed, commiting transaction.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("Block commit error: %v", err)
	}
	log.Printf("Block writer done.")
}

func pgTxWriter(c chan *txRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "txid", "version", "locktime"}
	bcols := []string{"block_id", "n", "tx_id"}

	txn, stmt, err := begin(db, "txs", cols)
	if err != nil {
		log.Printf("ERROR (3): %v", err)
	}

	btxn, bstmt, err := begin(db, "block_txs", bcols)
	if err != nil {
		log.Printf("ERROR (4): %v", err)
	}

	for tr := range c {
		if tr == nil || tr.tx == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			if err = commit(bstmt, btxn); err != nil {
				log.Printf("Block Txs commit error: %v", err)
			}
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
			continue
		}

		if !tr.dupe {
			t := tr.tx
			_, err = stmt.Exec(
				tr.id,
				tr.hash[:],
				int32(t.Version),
				int32(t.LockTime),
			)
			if err != nil {
				log.Printf("ERROR (7): %v", err)
			}
			// It can still be a dupe if we are catching up and the
			// cache is empty. In which case we will get a Tx commit
			// error below, which is fine.
		}

		_, err = bstmt.Exec(
			tr.blockId,
			tr.n,
			tr.id,
		)
		if err != nil {
			log.Printf("ERROR (7.5): %v", err)
		}

		if tr.sync != nil {
			// commit and send confirmation
			if err = commit(stmt, txn); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			if err = commit(bstmt, btxn); err != nil {
				log.Printf("Block Txs commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txs", cols)
			if err != nil {
				log.Printf("ERROR (8): %v", err)
			}
			btxn, bstmt, err = begin(db, "block_txs", bcols)
			if err != nil {
				log.Printf("ERROR (8.5): %v", err)
			}
			tr.sync <- true
		}
	}

	log.Printf("Tx writer channel closed, committing transaction.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("Tx commit error: %v", err)
	}
	if err = commit(bstmt, btxn); err != nil {
		log.Printf("Block Txs commit error: %v", err)
	}
	log.Printf("Tx writer done.")

}

func pgTxInWriter(c chan *txInRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "prevout_hash", "prevout_n", "scriptsig", "sequence", "witness", "prevout_tx_id"}

	txn, stmt, err := begin(db, "txins", cols)
	if err != nil {
		log.Printf("ERROR (9): %v", err)
	}

	for tr := range c {
		if tr == nil || tr.txIn == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("Txin commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txins", cols)
			if err != nil {
				log.Printf("ERROR (10): %v", err)
			}
			if tr != nil && tr.sync != nil {
				tr.sync <- true
			}
			continue
		}

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
		}

		_, err = stmt.Exec(
			tr.txId,
			tr.n,
			t.PrevOut.Hash[:],
			int32(t.PrevOut.N),
			t.ScriptSig,
			int32(t.Sequence),
			wb,
			prevOutTxId,
		)
		if err != nil {
			log.Printf("ERROR (11): %v", err)
		}

	}

	log.Printf("TxIn writer channel closed, committing transaction.")
	if err = commit(stmt, txn); err != nil {
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

	for tr := range c {

		if tr == nil || tr.txOut == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("TxOut commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txouts", cols)
			if err != nil {
				log.Printf("ERROR (13): %v", err)
			}
			if tr != nil && tr.sync != nil {
				tr.sync <- true
			}
			continue
		}

		var spent bool
		if utxo != nil {
			// TODO we probably do not need this when this is not the
			// first import, as the triggers should maintain the correct
			// spent status.
			isUTXO, err := utxo.IsUTXO(tr.hash, uint32(tr.n))
			if err != nil {
				log.Printf("ERROR (13.5): %v", err)
			}
			spent = !isUTXO
		}

		t := tr.txOut
		_, err = stmt.Exec(
			tr.txId,
			tr.n,
			t.Value,
			t.ScriptPubKey,
			spent,
		)
		if err != nil {
			log.Printf("ERROR (13.6): %v\n", err)
		}

	}

	log.Printf("TxOut writer channel closed, committing transaction.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("TxOut commit error: %v", err)
	}
	log.Printf("TxOut writer done.")
}

func commit(stmt *sql.Stmt, txn *sql.Tx) (err error) {
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func getHeightAndHashes(db *sql.DB) (int, []blkchain.Uint256, error) {

	stmt := "SELECT height, hash FROM blocks " +
		"WHERE height IN (SELECT MAX(height) FROM blocks) " +
		"ORDER BY id "

	rows, err := db.Query(stmt)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	var (
		height int
		hashes = make([]blkchain.Uint256, 0, 1)
	)

	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&height, &hash); err != nil {
			return 0, nil, err
		}
		hashes = append(hashes, blkchain.Uint256FromBytes(hash))
	}
	if len(hashes) > 0 {
		return height, hashes, nil
	}

	// Initial height is -1, so that 1st block is height 0
	return -1, nil, rows.Err()
}

func getLastBlockId(db *sql.DB) (int, error) {

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

func createTables(db *sql.DB) error {
	sqlTables := `
  CREATE TABLE blocks (
   id           SERIAL
  ,height       INT NOT NULL -- not same as id, because orphans.
  ,hash         BYTEA NOT NULL
  ,version      INT NOT NULL
  ,prevhash     BYTEA NOT NULL
  ,merkleroot   BYTEA NOT NULL
  ,time         INT NOT NULL
  ,bits         INT NOT NULL
  ,nonce        INT NOT NULL
  ,orphan       BOOLEAN NOT NULL DEFAULT false
  );

  CREATE TABLE txs (
   id            BIGSERIAL
  ,txid          BYTEA NOT NULL
  ,version       INT NOT NULL
  ,locktime      INT NOT NULL
  );

  CREATE TABLE block_txs (
   block_id      INT NOT NULL
  ,n             INT NOT NULL
  ,tx_id         BIGINT NOT NULL
  );

  CREATE TABLE txins (
   tx_id         BIGINT NOT NULL
  ,n             INT NOT NULL
  ,prevout_hash  BYTEA NOT NULL
  ,prevout_n     INT NOT NULL
  ,scriptsig     BYTEA NOT NULL
  ,sequence      INT NOT NULL
  ,witness       BYTEA
  ,prevout_tx_id BIGINT
  );

  CREATE TABLE txouts (
   tx_id        BIGINT NOT NULL
  ,n            INT NOT NULL
  ,value        BIGINT NOT NULL
  ,scriptpubkey BYTEA NOT NULL
  ,spent        BOOL NOT NULL
  );
`
	_, err := db.Exec(sqlTables)
	return err
}

func createIndexes1(db *sql.DB, verbose bool) error {
	// Adding a constraint or index if it does not exist is a little tricky in PG
	if verbose {
		log.Printf("  - blocks primary key...")
	}
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
		log.Printf("  - blocks prevhash index...")
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS blocks_prevhash_idx ON blocks(prevhash);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - blocks hash index...")
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS blocks_hash_idx ON blocks(hash);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - blocks height index...")
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS blocks_height_idx ON blocks(height);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txs primary key...")
	}
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
		log.Printf("  - txs txid (hash) index...")
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS txs_txid_idx ON txs(txid);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - block_txs block_id, n primary key...")
	}
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
		log.Printf("  - block_txs tx_id index...")
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS block_txs_tx_id_idx ON block_txs(tx_id);"); err != nil {
		return err
	}
	return nil
}

func createIndexes2(db *sql.DB, verbose bool) error {
	if verbose {
		log.Printf("  - txins (prevout_tx_id, prevout_tx_n) index...")
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS txins_prevout_tx_id_prevout_n_idx ON txins(prevout_tx_id, prevout_n);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txins primary key...")
	}
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
		log.Printf("  - txouts primary key...")
	}
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

	return nil
}

func createConstraints(db *sql.DB, verbose bool) error {
	if verbose {
		log.Printf("  - block_txs block_id foreign key...")
	}
	if _, err := db.Exec(`
	   DO $$
	   BEGIN
	     -- NB: table_name is the target/foreign table
	     IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
	                     WHERE table_name = 'blocks' AND constraint_name = 'block_txs_block_id_fkey') THEN
	       ALTER TABLE block_txs ADD CONSTRAINT block_txs_block_id_fkey FOREIGN KEY (block_id) REFERENCES blocks(id);
	     END IF;
	   END
	   $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - block_txs tx_id foreign key...")
	}
	if _, err := db.Exec(`
	   DO $$
	   BEGIN
	     -- NB: table_name is the target/foreign table
	     IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
	                     WHERE table_name = 'txs' AND constraint_name = 'block_txs_tx_id_fkey') THEN
	       ALTER TABLE block_txs ADD CONSTRAINT block_txs_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id);
	     END IF;
	   END
	   $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txins tx_id foreign key...")
	}
	if _, err := db.Exec(`
       DO $$
       BEGIN
         -- NB: table_name is the target/foreign table
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txins_tx_id_fkey') THEN
           ALTER TABLE txins ADD CONSTRAINT txins_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txouts tx_id foreign key...")
	}
	if _, err := db.Exec(`
       DO $$
       BEGIN
         -- NB: table_name is the target/foreign table
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txouts_tx_id_fkey') THEN
           ALTER TABLE txouts ADD CONSTRAINT txouts_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	return nil
}

// TODO: We already take care of this in leveldb.go?
//
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
	var limitSql string
	if limit > 0 {
		limitSql = fmt.Sprintf("WHERE n < %d", limit)
	}
	if _, err := w.db.Exec(fmt.Sprintf(`
UPDATE blocks
   SET orphan = a.orphan
  FROM (
    SELECT blocks.id, x.id IS NULL AS orphan
      FROM blocks
      LEFT JOIN (
        WITH RECURSIVE recur(id, prevhash) AS (
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
        SELECT recur.id, recur.prevhash, n
          FROM recur
      ) x ON blocks.id = x.id
   ) a
  WHERE blocks.id = a.id;
       `, limitSql)); err != nil {
		return err
	}
	return nil
}

// Most of the prevout_tx_id's should be already set during the
// import, but we need to correct the remaining ones. This is a fairly
// costly operation as it requires a txins table scan. You can likely
// avoid it by increasing the txIdCache size. After the first import
// it will be maintained by triggers.
func fixPrevoutTxId(db *sql.DB) error {
	if _, err := db.Exec(`
       DO $$
       BEGIN
         -- existence of txins_pkey means it is already done
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txins_tx_id_fkey') THEN
           UPDATE txins i
              SET prevout_tx_id = t.id
             FROM txs t
            WHERE i.prevout_hash = t.txid
              AND i.prevout_tx_id IS NULL
              AND i.n <> -1;

         END IF;
       END
       $$`); err != nil {
		return err
	}
	return nil
}

func createTxinsTriggers(db *sql.DB) error {
	if _, err := db.Exec(`
CREATE OR REPLACE FUNCTION txins_before_trigger_func() RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'UPDATE' OR TG_OP = 'INSERT') THEN
      IF NEW.prevout_n <> -1 AND NEW.prevout_tx_id IS NULL THEN
        SELECT id INTO NEW.prevout_tx_id FROM txs WHERE txid = NEW.prevout_hash;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'Unknown prevout_hash %', NEW.prevout_hash;
        END IF;
      END IF;
      RETURN NEW;
    END IF;
    RETURN NULL;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER txins_before_trigger
BEFORE INSERT OR UPDATE OR DELETE ON txins
  FOR EACH ROW EXECUTE PROCEDURE txins_before_trigger_func();

CREATE OR REPLACE FUNCTION txins_after_trigger_func() RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      IF OLD.prevout_tx_id IS NOT NULL THEN
        UPDATE txouts SET spent = FALSE
         WHERE tx_id = prevout_tx_id AND n = OLD.prevout_n;
      END IF;
      RETURN OLD;
    ELSIF (TG_OP = 'UPDATE' OR TG_OP = 'INSERT') THEN
      IF NEW.prevout_tx_id IS NOT NULL THEN
        UPDATE txouts SET spent = TRUE
         WHERE tx_id = NEW.prevout_tx_id AND n = NEW.prevout_n;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'Unknown prevout_n % in txid % (id %)', NEW.prevout_n, NEW.prevout_hash, NEW.prevout_tx_id;
        END IF;
      END IF;
      RETURN NEW;
    END IF;
    RETURN NULL;
  END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER txins_after_trigger
AFTER INSERT OR UPDATE OR DELETE ON txins DEFERRABLE
  FOR EACH ROW EXECUTE PROCEDURE txins_after_trigger_func();

        `); err != nil {
		return err
	}
	return nil
}
