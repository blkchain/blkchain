package db

import (
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
