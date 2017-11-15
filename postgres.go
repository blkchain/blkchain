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
}

type txRec struct {
	id      int
	blockId int
	tx      *Tx
}

type txInRec struct {
	txId int
	n    int
	txIn *TxIn
}

type txOutRec struct {
	txId  int
	n     int
	txOut *TxOut
}

func NewPGWriter(connstr string) (chan *Block, *sync.WaitGroup, error) {

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
			return nil, nil, err
		}
	}

	ch := make(chan *Block, 64)
	go pgBlockWorker(ch, &wg, db, deferredIndexes)

	return ch, &wg, nil
}

func pgBlockWorker(ch chan *Block, wg *sync.WaitGroup, db *sql.DB, deferredIndexes bool) {

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

	blockCh := make(chan *blockRec, 1024)
	go pgBlockWriter(blockCh, db)

	txCh := make(chan *txRec, 1024)
	go pgTxWriter(txCh, db)

	txInCh := make(chan *txInRec, 1024)
	go pgTxInWriter(txInCh, db)

	txOutCh := make(chan *txOutRec, 1024)
	go pgTxOutWriter(txOutCh, db)

	writerWg.Add(4)

	start := time.Now()

	if len(bhash) > 0 {
		log.Printf("Skipping to hash %v", uint256FromBytes(bhash))
		skip := 0
		for b := range ch {
			hash := b.Hash()
			if bytes.Compare(bhash, hash[:]) == 0 {
				break
			} else {
				skip++
				if skip%50000 == 0 {
					log.Printf("Skipped %d blocks", skip)
				}
			}
		}
		log.Printf("Skipped %d blocks", skip)
	}

	txcnt := 0
	for b := range ch {
		bid++
		height++
		blockCh <- &blockRec{
			id:     bid,
			prevId: bid - 1,
			height: height,
			block:  b,
		}

		for _, tx := range b.Txs {
			txid++
			txcnt++
			txCh <- &txRec{
				id:      txid,
				blockId: bid,
				tx:      tx,
			}

			for n, txin := range tx.TxIns {
				txInCh <- &txInRec{
					txId: txid,
					n:    n,
					txIn: txin,
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

		if bid%60 == 0 {
			blockCh <- nil
			txInCh <- nil
			txOutCh <- nil
			txCh <- nil

			log.Printf("%v COMMIT at Height: %d Txs: %d Time: %v Tx/s: %02f\n",
				time.Now(), bid, txcnt, time.Unix(int64(b.Time), 0), float64(txcnt)/time.Now().Sub(start).Seconds())
		}
	}

	close(blockCh)
	close(txInCh)
	close(txOutCh)
	close(txCh)

	log.Printf("Closed db channels, waiting for workers to finish...")
	writerWg.Wait()
	log.Printf("Workers finished.")

	if deferredIndexes {
		log.Printf("Creating indexes, please be patient, this may take a long time...")
		if err := createIndexes(db); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}
		log.Printf("Creating constraints, please be patient, this may take a long time...")
		if err := createConstraints(db); err != nil {
			log.Printf("Error creating constraints: %v", err)
		}
		log.Printf("Indexes and constraints created.")
	}
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
		hash := t.Hash()
		_, err = stmt.Exec(
			tr.id,
			tr.blockId,
			hash[:],
			int32(t.Version),
			int32(t.LockTime),
		)
		if err != nil {
			log.Printf("ERROR (5): %v", err)
		}

	}

	log.Printf("Tx writer channel closed, leaving.")
	if err = commit(stmt, txn); err != nil {
		log.Printf("Tx commit error: %v", err)
	}

}

func pgTxInWriter(c chan *txInRec, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"tx_id", "n", "prevout_hash", "prevout_n", "scriptsig", "sequence", "witness"}

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

		_, err = stmt.Exec(
			tr.txId,
			tr.n,
			t.PrevOut.Hash[:],
			int32(t.PrevOut.N),
			t.ScriptSig,
			int32(t.Sequence),
			wb,
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

func getLastTxId(db *sql.DB) (int, error) {

	rows, err := db.Query("SELECT id FROM txs ORDER BY id DESC LIMIT 1")
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

func createTables(db *sql.DB) error {
	sqlTables := `
  CREATE TABLE blocks (
   id           SERIAL
  ,prev         INT NOT NULL
  ,height       INT NOT NULL -- not same as id, because orphans
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

func createIndexes(db *sql.DB) error {
	sqlIndexes := `
    ALTER TABLE blocks ADD PRIMARY KEY(id);
    ALTER TABLE txs ADD PRIMARY KEY(id);
    ALTER TABLE txins ADD PRIMARY KEY(tx_id, n);
    ALTER TABLE txouts ADD PRIMARY KEY(tx_id, n);
    CREATE INDEX ON txs(block_id);
    -- CREATE INDEX ON txs(txid);
    CREATE INDEX ON txs(substring(txid for 8));
`
	_, err := db.Exec(sqlIndexes)
	return err
}

func createConstraints(db *sql.DB) error {
	sqlConstraints := `
    ALTER TABLE txs
      ADD CONSTRAINT fk_txs_block_id
      FOREIGN KEY (block_id)
      REFERENCES blocks(id);

    ALTER TABLE txins
      ADD CONSTRAINT fk_txins_tx_id
      FOREIGN KEY (tx_id)
      REFERENCES txs(id);

    ALTER TABLE txouts
      ADD CONSTRAINT fk_txouts_tx_id
      FOREIGN KEY (tx_id)
      REFERENCES txs(id);
  `
	_, err := db.Exec(sqlConstraints)
	return err
}
