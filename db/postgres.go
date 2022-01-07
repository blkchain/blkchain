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
	blockCh chan *blockRecSync
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

	if err := createPgcrypto(db); err != nil {
		return nil, err
	}

	firstImport := true // firstImport == first import
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
	}

	if err := createPrevoutMissTable(db); err != nil {
		return nil, err
	}

	bch := make(chan *blockRecSync, 2)
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

	txcnt, start, last := 0, time.Now(), time.Now()
	blkCnt, blkSz := 0, 0
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

		blkSz += br.Size()
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
		} else if bid%30 == 0 {
			// commit every N blocks
			blockCh <- nil
			txCh <- nil
			txInCh <- nil
			txOutCh <- nil
		}

		// report progress
		if time.Now().Sub(last) > 5*time.Second {
			log.Printf("Height: %d Txs: %d Time: %v Tx/s: %02f KB/s: %02f",
				br.Height, txcnt,
				time.Unix(int64(br.Time), 0),
				float64(txcnt)/time.Now().Sub(start).Seconds(),
				float64(blkSz/1024)/time.Now().Sub(start).Seconds())
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
		log.Printf("Creating indexes (if needed), please be patient, this may take a long time...")
		if err := createIndexes(w.db, verbose); err != nil {
			log.Printf("Error creating indexes: %v", err)
		}

		log.Printf("Creating constraints (if needed), please be patient, this may take a long time...")
		if err := createConstraints(w.db, verbose); err != nil {
			log.Printf("Error creating constraints: %v", err)
		}

		log.Printf("Creating txins triggers.")
		if err := createTxinsTriggers(w.db); err != nil {
			log.Printf("Error creating txins triggers: %v", err)
		}

		if idCache.miss > 0 {
			log.Printf("Fixing missing prevout_tx_id entries (if needed), this may take a long time...")
			// NB: The ANALYZE below seems to make no difference when _prvout_miss is relatively small
			// log.Printf("  running ANALYZE txins, _prevout_miss, txs to ensure the next step selects the optimal plan...")
			// if err := fixPrevoutTxIdAnalyze(w.db); err != nil {
			// 	log.Printf("Error running ANALYZE: %v", err)
			// }
			if err := fixPrevoutTxId(w.db); err != nil {
				log.Printf("Error fixing prevout_tx_id: %v", err)
			}
		} else {
			log.Printf("NOT fixing missing prevout_tx_id entries because there were 0 cache misses.")
		}
	}

	log.Printf("Dropping _prevout_miss table.")
	if err := dropPrevoutMissTable(w.db); err != nil {
		log.Printf("Error dropping _prevout_miss table: %v", err)
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

func pgBlockWriter(c chan *blockRecSync, db *sql.DB) {
	defer writerWg.Done()

	cols := []string{"id", "height", "hash", "version", "prevhash", "merkleroot", "time", "bits", "nonce", "orphan", "size", "base_size", "weight", "virt_size"}

	txn, stmt, err := begin(db, "blocks", cols)
	if err != nil {
		log.Printf("ERROR (1): %v", err)
	}

	for br := range c {

		if br == nil || br.BlockRec == nil { // commit signal
			if err = commit(stmt, txn, nil); err != nil {
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

		b := br.Block
		_, err = stmt.Exec(
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
		)
		if err != nil {
			log.Printf("ERROR (3): %v", err)
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

	for tr := range c {
		if tr == nil || tr.tx == nil { // commit signal
			if err = commit(stmt, txn, nil); err != nil {
				log.Printf("Tx commit error: %v", err)
			}
			if err = commit(bstmt, btxn, nil); err != nil {
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
				t.Size(),
				t.BaseSize(),
				t.Weight(),
				t.VirtualSize(),
			)
			if err != nil {
				log.Printf("ERROR (7): %v", err)
			}
			// It can still be a dupe if we are catching up and the
			// cache is empty, which is why we warmupCache.
		}

		_, err = bstmt.Exec(
			tr.blockId,
			tr.n,
			tr.id,
		)
		if err != nil {
			log.Printf("ERROR (7.5): %v", err)
		}
	}

	log.Printf("Tx writer channel closed, committing transaction.")

	if err = commit(stmt, txn, nil); err != nil {
		log.Printf("Tx commit error: %v", err)
	}
	if err = commit(bstmt, btxn, nil); err != nil {
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

	for tr := range c {
		if tr == nil || tr.txIn == nil { // commit signal
			if err = commit(stmt, txn, misses); err != nil {
				log.Printf("Txin commit error: %v", err)
			}
			misses = misses[:0]
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

			if prevOutTxId == nil { // cache miss
				if firstImport {
					// write it to the DB directly
					if err := recordPrevoutMiss(db, tr.txId, tr.n, t.PrevOut.Hash); err != nil {
						log.Printf("ERROR (10.7): %v", err)
					}
				} else {
					// remember it, it will be written just when needed
					misses = append(misses, &prevoutMiss{tr.txId, tr.n, t.PrevOut.Hash})
				}
			}
		}

		_, err = stmt.Exec(
			tr.txId,
			tr.n,
			prevOutTxId,
			int32(t.PrevOut.N),
			t.ScriptSig,
			int32(t.Sequence),
			wb,
		)
		if err != nil {
			log.Printf("ERROR (11): %v", err)
		}
	}

	log.Printf("TxIn writer channel closed, committing transaction.")
	if err = commit(stmt, txn, misses); err != nil {
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
			if err = commit(stmt, txn, nil); err != nil {
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
			// NB: Some early unspent coins are not in the UTXO set,
			// probably because they are not spendable? So the spent
			// flag could also be interpeted as unspendable.
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
	if err = commit(stmt, txn, nil); err != nil {
		log.Printf("TxOut commit error: %v", err)
	}
	log.Printf("TxOut writer done.")
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

func commit(stmt *sql.Stmt, txn *sql.Tx, misses []*prevoutMiss) (err error) {
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	// We do this here because it is not possible to use Exec() during
	// a pq.CopyIn operation, but we do want this done prior to the
	// Commit.
	if len(misses) > 0 {
		if err = recordPrevoutMisses(txn, misses); err != nil {
			return err
		}
		if err = fixPrevoutTxId(txn); err != nil { // also truncates _prevout_miss
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

	stmt := `SELECT height, hash FROM blocks
		WHERE height > (SELECT MAX(height) FROM blocks) - $1`

	rows, err := db.Query(stmt, back)
	if err != nil {
		return nil, err
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

// This could be a db connection or a transaction
type execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func createPrevoutMissTable(db execer) error {
	sql := `
     CREATE UNLOGGED TABLE IF NOT EXISTS _prevout_miss (
          tx_id         BIGINT NOT NULL
         ,n             INT NOT NULL
         ,prevout_hash  BYTEA NOT NULL
     );
     TRUNCATE _prevout_miss;
`
	_, err := db.Exec(sql)
	return err
}

func clearPrevoutMissTable(db execer) error {
	_, err := db.Exec("TRUNCATE _prevout_miss")
	return err
}

func dropPrevoutMissTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS _prevout_miss")
	return err
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
// still important.
func fixPrevoutTxId(db execer) error {
	if _, err := db.Exec(`
           UPDATE txins i
              SET prevout_tx_id = t.id
             FROM _prevout_miss m
             JOIN txs t ON m.prevout_hash = t.txid
            WHERE i.tx_id = m.tx_id
              AND i.n = m.n
        `); err != nil {
		return err
	}
	return clearPrevoutMissTable(db)
}

// func fixPrevoutTxIdAnalyze(db execer) error {
// 	_, err := db.Exec(`ANALYZE txins, _prevout_miss, txs`)
// 	return err
// }

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

func createPgcrypto(db *sql.DB) error {
	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto")
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
	if verbose {
		log.Printf("  - hash_type function...")
	}
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
	if verbose {
		log.Printf("  - txouts address prefix index...")
	}
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
           $$ LANGUAGE plpgsql IMMUTABLE;

           -- Address prefix (txout)
           CREATE OR REPLACE FUNCTION addr_prefix(scriptPubKey BYTEA) RETURNS BIGINT AS $$
           BEGIN
             RETURN bytes2int8(extract_address(scriptPubKey));
           END;
           $$ LANGUAGE plpgsql IMMUTABLE;

           CREATE INDEX IF NOT EXISTS txouts_addr_prefix_tx_id_idx ON txouts(addr_prefix(scriptpubkey), tx_id);
       `); err != nil {
		return err
	}
	if verbose {
		log.Printf("  - txins address prefix index...")
	}
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
        $$ LANGUAGE plpgsql IMMUTABLE;

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
        $$ LANGUAGE plpgsql IMMUTABLE;

        -- Address prefix (txin)
        CREATE OR REPLACE FUNCTION addr_prefix(scriptsig BYTEA, witness BYTEA) RETURNS BIGINT AS $$
        BEGIN
          RETURN public.bytes2int8(public.extract_address(scriptsig, witness));
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;

        -- Partial/conditional index because coinbase txin scriptsigs are garbage
        CREATE INDEX IF NOT EXISTS txins_addr_prefix_tx_id_idx ON txins(addr_prefix(scriptsig, witness), tx_id)
         WHERE prevout_tx_id IS NOT NULL;
       `); err != nil {
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
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER txins_after_trigger
AFTER INSERT OR UPDATE OR DELETE ON txins DEFERRABLE
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
