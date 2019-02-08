package db

import (
	"fmt"

	"github.com/blkchain/blkchain"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	ConnectString string
}

type Explorer struct {
	db *sqlx.DB
}

func NewExplorer(cfg Config) (*Explorer, error) {
	if conn, err := sqlx.Connect("postgres", cfg.ConnectString); err != nil {
		return nil, err
	} else {
		e := &Explorer{db: conn}
		if err := e.db.Ping(); err != nil {
			return nil, err
		}
		return e, nil
	}
}

func (e *Explorer) SelectBlocksJson(height, limit int) ([]string, error) {
	stmt := "SELECT to_json(b.*) AS block " +
		"FROM (SELECT height, hash, version, prevhash, merkleroot, time, bits, nonce, orphan " +
		"FROM blocks " +
		"WHERE height <= $1 " +
		"ORDER BY height DESC LIMIT $2 ) b"

	var blocks []string
	if err := e.db.Select(&blocks, stmt, height, limit); err != nil {
		return nil, err
	}

	return blocks, nil
}

func (e *Explorer) SelectMaxHeight() (int, error) {

	stmt := "SELECT MAX(height) AS height FROM blocks"

	var height int
	if err := e.db.Get(&height, stmt); err != nil {
		return 0, err
	}

	return height, nil
}

func (e *Explorer) SelectBlockByHashJson(hash blkchain.Uint256) (*string, error) {
	stmt := "SELECT to_json(b.*) AS block FROM ( " +
		"SELECT height, hash, version, prevhash, merkleroot, time, bits, nonce, orphan " +
		"FROM blocks " +
		"WHERE hash = $1 " +
		") b"

	var block string
	if err := e.db.Get(&block, stmt, hash[:]); err != nil {
		return nil, err
	}

	return &block, nil
}

func (e *Explorer) SelectTxsJson(blockHash blkchain.Uint256, startN, limit int) ([]string, error) {
	// NB: unlike with SelectTxByHashJson, we do not support negative
	// limit here. This is because the block url structure in order to
	// be cacheable is such that there is only one way to denote a
	// section of a block (with positive limit).
	stmt := `SELECT to_json(t.*) AS tx
  FROM (
    SELECT bt.n, t.txid, t.version, t.locktime
      FROM blocks b
      JOIN block_txs bt ON b.id = bt.block_id
      JOIN txs t ON t.id = bt.tx_id
     WHERE b.hash = $1
       AND bt.n >= $2
     ORDER BY bt.n
     LIMIT $3
  ) t`

	var txs []string
	if err := e.db.Select(&txs, stmt, blockHash[:], startN, limit); err != nil {
		return nil, err
	}

	return txs, nil
}

func (e *Explorer) SelectTxByHashJson(hash blkchain.Uint256) (*string, error) {
	// This statement uses a bit of cleverness to hide the internal
	// db ids, not sure if it was necessary.
	stmt := `
SELECT to_json(t.*) FROM (
SELECT txid
       , t.version
       , i.ins AS inputs
       , o.outs AS outputs
       , t.locktime
       , blocks
  FROM txs t
  JOIN LATERAL (
    SELECT ARRAY_AGG(i.*  ORDER BY n) AS ins
      FROM (
        SELECT n, ts.txid AS prevout_hash, prevout_n, scriptsig, sequence, witness
          FROM txins ti
          JOIN txs ts ON ti.prevout_tx_id = ts.id
         WHERE tx_id = t.id
      ) i
  ) i ON true
  JOIN LATERAL (
    SELECT ARRAY_AGG(o.*  ORDER BY n) AS outs
      FROM (
        SELECT n, value, scriptpubkey, spent
          FROM txouts
         WHERE tx_id = t.id
      ) o
  ) o ON true
  JOIN block_txs bt ON t.id = bt.tx_id
  JOIN LATERAL (
    SELECT ARRAY_AGG(hash) AS blocks
      FROM blocks b
     WHERE b.id = bt.block_id
  ) b ON true
WHERE t.txid = $1
) t;
`
	var tx string
	if err := e.db.Get(&tx, stmt, hash[:]); err != nil {
		return nil, err
	}

	return &tx, nil
}

func (e *Explorer) SelectHashType(hash blkchain.Uint256) (*string, error) {
	stmt := "SELECT hash_type($1)"

	var typ *string
	if err := e.db.Get(&typ, stmt, hash[:]); err != nil {
		return nil, err
	}

	return typ, nil
}

func (e *Explorer) SelectTxsByAddrJson(addr []byte, startTxId int, limit int) ([]string, error) {
	// Negative limit changes the direction and the condition. (Though
	// in the end rows are ordered DESC). The operator is exclusive
	// (not >= or <=), so the start tx id is never in the result set.
	//
	// The first part of this query gets the tx ids we need by using
	// the expression indexes on txins and txouts and orders them by
	// tx_id. The second part adds the rest of the transaction
	// data. Even though this seems like a lot, the heavylifting is
	// done by the first part, after that all the tuples we need are
	// already in memory.

	operator, order := "<", "DESC"
	if limit < 0 {
		operator, limit, order = ">", -limit, "ASC"
	}
	stmt := fmt.Sprintf(`
SELECT to_json(txs.*) FROM (
SELECT t.id, t.txid, t.version, t.inputs, t.outputs, t.locktime, t.blocks FROM (
  ( SELECT tx_id
      FROM txins i
     WHERE addr_prefix(scriptsig, witness) = bytes2int8($1)
       AND prevout_tx_id IS NOT NULL
       AND extract_address(scriptsig, witness) = $1
       AND i.tx_id %[1]s $2
     ORDER BY tx_id %[2]s
    LIMIT $3
  )
  UNION
  ( SELECT tx_id
      FROM txouts o
     WHERE addr_prefix(scriptpubkey) = bytes2int8($1)
       AND extract_address(scriptpubkey) = $1
       AND o.tx_id %[1]s $2
     ORDER BY tx_id %[2]s
    LIMIT $3
  )
ORDER BY tx_id %[2]s
LIMIT $3
) tid
JOIN LATERAL (
  SELECT id, txid, t.version, i.ins AS inputs, o.outs AS outputs, t.locktime, b.blocks
  FROM txs t
  JOIN LATERAL (
    SELECT ARRAY_AGG(i.*  ORDER BY n) AS ins
      FROM (
        SELECT n, ts.txid AS prevout_hash, prevout_n, scriptsig, sequence, witness
          FROM txins ti
          JOIN txs ts ON ti.prevout_tx_id = ts.id
         WHERE tx_id = t.id
      ) i
  ) i ON true
  JOIN LATERAL (
    SELECT ARRAY_AGG(o.*  ORDER BY n) AS outs
      FROM (
        SELECT n, value, scriptpubkey, spent
          FROM txouts
         WHERE tx_id = t.id
      ) o
  ) o ON true
  JOIN block_txs bt ON t.id = bt.tx_id
  JOIN LATERAL (
    SELECT ARRAY_AGG(hash) AS blocks
      FROM blocks b
     WHERE b.id = bt.block_id
  ) b ON true
  WHERE t.id = tid.tx_id
  ) t ON true
  ORDER BY tx_id DESC -- correct
) txs;
`, operator, order)
	var txs []string
	if err := e.db.Select(&txs, stmt, addr, startTxId, limit); err != nil {
		return nil, err
	}

	return txs, nil
}

func (e *Explorer) SelectAddrTotalReceived(addr []byte, limit int) (int64, error) {
	// This query adds all the txouts to the given address, but
	// subtracts the value that a corresponding txin had iff it was
	// from the same address (unless the difference is negative). This
	// query cannot work for address associated with a lot of txouts,
	// so if the limit is hit, the method returns -1 meaning "too many
	// txouts to count".
	stmt := `
SELECT SUM(recv) AS recv, COUNT(1) AS cnt FROM (
  SELECT tx_id, o.n, CASE WHEN (o.value - self) < 0 THEN 0 ELSE (o.value-self) END AS recv
      FROM txouts o
      JOIN LATERAL (
        SELECT COALESCE(SUM(oo.value), 0) AS self
          FROM txins i
          JOIN txouts oo ON i.prevout_tx_id = oo.tx_id AND i.prevout_n = oo.n
        WHERE i.tx_id = o.tx_id
          AND extract_address(scriptsig, witness) = $1
     ) x ON true
     WHERE addr_prefix(scriptpubkey) = bytes2int8($1)
       AND extract_address(scriptpubkey) = $1
  LIMIT $2
) x;
`
	type recvCnt struct {
		Recv int64
		Cnt  int
	}
	var recv recvCnt
	if err := e.db.Get(&recv, stmt, addr, limit+1); err != nil {
		return 0, err
	}

	if recv.Cnt > limit { // To many rows
		return -1, nil
	}

	return recv.Recv, nil
}
