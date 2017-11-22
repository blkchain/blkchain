package blkchain

import (
	"bytes"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

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
	prevId int
	height int
	block  *Block
	sync   chan bool
}

type txRec struct {
	id      int64
	blockId int
	tx      *Tx
	hash    Uint256
	sync    chan bool
}

type txInRec struct {
	txId    int64
	n       int
	txIn    *TxIn
	idCache *txIdCache
}

type txOutRec struct {
	txId  int64
	n     int
	txOut *TxOut
}

func NewPGWriter(connstr string, cacheSize int) (chan *Block, *sync.WaitGroup, error) {

	var wg sync.WaitGroup

	db, err := sql.Open("postgres", connstr)
	if err != nil {
		return nil, nil, err
	}

	deferredIndexes := true
	if err := createTables(db); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// this is fine, cancel deferred index/constraint creation
			deferredIndexes = false
		} else {
			log.Printf("Tables created without indexes, which are created at the very end.")
			return nil, nil, err
		}
	}

	ch := make(chan *Block, 64)
	go pgBlockWorker(ch, &wg, db, deferredIndexes, cacheSize)

	return ch, &wg, nil
}

func pgBlockWorker(ch chan *Block, wg *sync.WaitGroup, db *sql.DB, deferredIndexes bool, cacheSize int) {

	wg.Add(1)
	defer wg.Done()

	bid, height, bhash, err := getLastHashAndHeight(db)
	if err != nil {
		log.Printf("Error getting last hash and height, exiting: %v", err)
		return
	}
	txid, err := getLastTxId(db)
	if err != nil {
		log.Printf("Error getting last tx id, exiting: %v", err)
		return
	}

	blockCh := make(chan *blockRec, 64)
	go pgBlockWriter(blockCh, db)

	txCh := make(chan *txRec, 64)
	go pgTxWriter(txCh, db)

	txInCh := make(chan *txInRec, 64)
	go pgTxInWriter(txInCh, db)

	txOutCh := make(chan *txOutRec, 64)
	go pgTxOutWriter(txOutCh, db)

	writerWg.Add(4)

	start := time.Now()

	if len(bhash) > 0 {
		log.Printf("Skipping to hash %v", uint256FromBytes(bhash))
		skip, last := 0, time.Now()
		for b := range ch {
			hash := b.Hash()
			if bytes.Compare(bhash, hash[:]) == 0 {
				break
			} else {
				skip++
				if skip%10 == 0 && time.Now().Sub(last) > 5*time.Second {
					log.Printf("Skipped %d blocks...", skip)
					last = time.Now()
				}
			}
		}
		log.Printf("Skipped %d total blocks.", skip)
	}

	idCache := newTxIdCache(cacheSize)

	var syncCh chan bool
	if !deferredIndexes {
		// no deferredIndexes means that the constraints already
		// exist, and we need to wait for a tx to be commited before
		// ins/outs can be inserted. Same with block/tx.
		syncCh = make(chan bool, 0)
	}

	txcnt, last := 0, time.Now()
	for b := range ch {
		bid++
		height++
		blockCh <- &blockRec{
			id:     bid,
			prevId: bid - 1,
			height: height,
			block:  b,
			sync:   syncCh,
		}
		if syncCh != nil {
			<-syncCh
		}

		for _, tx := range b.Txs {
			txid++
			txcnt++

			hash := tx.Hash()

			if len(tx.TxIns) > 0 && tx.TxIns[0].PrevOut.N != 0xffffffff {
				idCache.add(hash, txid, len(tx.TxOuts))
			}

			txCh <- &txRec{
				id:      txid,
				blockId: bid,
				tx:      tx,
				hash:    hash,
				sync:    syncCh,
			}
			if syncCh != nil {
				<-syncCh
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
				}
			}
		}

		if !deferredIndexes {
			// commit after every block
			// blocks and txs are already commited
			txInCh <- nil
			txOutCh <- nil
		} else if bid%50 == 0 {
			// commit every N blocks
			blockCh <- nil
			txCh <- nil
			txInCh <- nil
			txOutCh <- nil
		}

		// report progress
		if time.Now().Sub(last) > 5*time.Second {
			log.Printf("Height: %d Txs: %d Time: %v Tx/s: %02f\n",
				height, txcnt, time.Unix(int64(b.Time), 0), float64(txcnt)/time.Now().Sub(start).Seconds())
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

	log.Printf("Txid cache hits: %d (%.02f%%) misses: %d collisions: %d evictions: %d",
		idCache.hits, float64(idCache.hits)/(float64(idCache.hits+idCache.miss)+0.0001)*100,
		idCache.miss, idCache.cols, idCache.evic)

	verbose := deferredIndexes
	log.Printf("Creating indexes (if needed), please be patient, this may take a long time...")
	if err := createIndexes(db, verbose); err != nil {
		log.Printf("Error creating indexes: %v", err)
	}
	log.Printf("Creating constraints (if needed), please be patient, this may take a long time...")
	if err := createConstraints(db, verbose); err != nil {
		log.Printf("Error creating constraints: %v", err)
	}
	log.Printf("Indexes and constraints created.")
}

func begin(db *sql.DB, table string, cols []string) (*sql.Tx, *sql.Stmt, error) {
	txn, err := db.Begin()
	if err != nil {
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

	cols := []string{"id", "prev", "height", "hash", "version", "prevhash", "merkleroot", "time", "bits", "nonce"}

	txn, stmt, err := begin(db, "blocks", cols)
	if err != nil {
		log.Printf("ERROR (1): %v", err)
	}

	for br := range c {

		if br == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("Block commit error: %v", err)
			}
			txn, stmt, err = begin(db, "blocks", cols)
			if err != nil {
				log.Printf("ERROR (2): %v", err)
			}
			continue
		}

		b := br.block
		hash := b.Hash()
		_, err = stmt.Exec(
			br.id,
			br.prevId,
			br.height,
			hash[:],
			int32(b.Version),
			b.PrevHash[:],
			b.HashMerkleRoot[:],
			int32(b.Time),
			int32(b.Bits),
			int32(b.Nonce),
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

	log.Printf("Block writer channel closed, leaving.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("Block commit error: %v", err)
	}

}

func pgTxWriter(c chan *txRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "block_id", "txid", "version", "locktime"}

	txn, stmt, err := begin(db, "txs", cols)
	if err != nil {
		log.Printf("ERROR (3): %v", err)
	}

	for tr := range c {
		if tr == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txs", cols)
			if err != nil {
				log.Printf("ERROR (4): %v", err)
			}
			continue
		}

		t := tr.tx
		_, err = stmt.Exec(
			tr.id,
			tr.blockId,
			tr.hash[:],
			int32(t.Version),
			int32(t.LockTime),
		)
		if err != nil {
			log.Printf("ERROR (5): %v", err)
		}

		if tr.sync != nil {
			// commit and send confirmation
			if err = commit(stmt, txn); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txs", cols)
			if err != nil {
				log.Printf("ERROR (4.5): %v", err)
			}
			tr.sync <- true
		}
	}

	log.Printf("Tx writer channel closed, leaving.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("Tx commit error: %v", err)
	}

}

func pgTxInWriter(c chan *txInRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "prevout_hash", "prevout_n", "scriptsig", "sequence", "witness", "prevout_tx_id"}

	txn, stmt, err := begin(db, "txins", cols)
	if err != nil {
		log.Printf("ERROR (6): %v", err)
	}

	for tr := range c {
		if tr == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("Txin commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txins", cols)
			if err != nil {
				log.Printf("ERROR (7): %v", err)
			}
			continue
		}

		t := tr.txIn
		var wb interface{}
		if t.Witness != nil {
			var b bytes.Buffer
			BinWrite(&t.Witness, &b)
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
			log.Printf("ERROR (8): %v", err)
		}

	}

	log.Printf("TxIn writer channel closed, leaving.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("TxIn commit error: %v", err)
	}
}

func pgTxOutWriter(c chan *txOutRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "value", "scriptpubkey"}

	txn, stmt, err := begin(db, "txouts", cols)
	if err != nil {
		log.Printf("ERROR (9): %v", err)
	}

	for tr := range c {

		if tr == nil { // commit signal
			if err = commit(stmt, txn); err != nil {
				log.Printf("TxOut commit error: %v", err)
			}
			txn, stmt, err = begin(db, "txouts", cols)
			if err != nil {
				log.Printf("ERROR (10): %v", err)
			}
			continue
		}

		t := tr.txOut
		_, err = stmt.Exec(
			tr.txId,
			tr.n,
			t.Value,
			t.ScriptPubKey,
		)
		if err != nil {
			log.Printf("ERROR (11): %v\n", err)
		}

	}

	log.Printf("TxOut writer channel closed, leaving.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("TxOut commit error: %v", err)
	}
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

func getLastHashAndHeight(db *sql.DB) (int, int, []byte, error) {

	rows, err := db.Query("SELECT id, height, hash FROM blocks ORDER BY height DESC LIMIT 1")
	if err != nil {
		return 0, 0, nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var (
			id     int
			height int
			hash   []byte
		)
		if err := rows.Scan(&id, &height, &hash); err != nil {
			return 0, 0, nil, err
		}
		return id, height, hash, nil
	}
	return 0, 0, nil, rows.Err()
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
  ,prev         INT NOT NULL
  ,height       INT NOT NULL -- not same as id, because orphans.
  ,hash         BYTEA NOT NULL
  ,version      INT NOT NULL
  ,prevhash     BYTEA NOT NULL
  ,merkleroot   BYTEA NOT NULL
  ,time         INT NOT NULL
  ,bits         INT NOT NULL
  ,nonce        INT NOT NULL
  );

  CREATE TABLE txs (
   id            BIGSERIAL
  ,block_id      INT NOT NULL
  ,txid          BYTEA NOT NULL -- the hash, cannot be unique
  ,version       INT NOT NULL
  ,locktime      INT NOT NULL
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
  );
`
	_, err := db.Exec(sqlTables)
	return err
}

func createIndexes(db *sql.DB, verbose bool) error {
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
		log.Printf("  - txs block_id index...")
	}
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS txs_block_id_idx ON txs(block_id);"); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txs txid (hash) index...")
	}
	// _, err := db.Exec("-- CREATE INDEX IF NOT EXISTS txs_txid_idx ON txs(txid);")
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS txs_substring_idx ON txs(substring(txid for 8));"); err != nil {
		return err
	}
	// Expression indexes require analyze?
	//log.Printf("  - analyzing txs table...")
	//_, err := db.Exec("ANALYZE txs;")
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
	// NB: table_name is the target/foreign table!
	if verbose {
		log.Printf("  - txs block_id foreign key...")
	}
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'blocks' AND constraint_name = 'txs_block_id_fkey') THEN
           ALTER TABLE txs ADD CONSTRAINT txs_block_id_fkey FOREIGN KEY (block_id) REFERENCES blocks(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txsins tx_id foreign key...")
	}
	if _, err := db.Exec(`
       DO $$
       BEGIN
         IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage
                         WHERE table_name = 'txs' AND constraint_name = 'txins_tx_id_fkey') THEN
           ALTER TABLE txins ADD CONSTRAINT txins_tx_id_fkey FOREIGN KEY (tx_id) REFERENCES txs(id);
         END IF;
       END
       $$;`); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txsins tx_id foreign key...")
	}
	if _, err := db.Exec(`
       DO $$
       BEGIN
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
