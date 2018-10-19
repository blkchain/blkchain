package db

import (
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

func (e *Explorer) SelectBlocks(height, limit int) ([]*BlockRec, error) {
	stmt := "SELECT id, height, hash, version, prevhash, merkleroot, time, bits, nonce, orphan " +
		"FROM blocks " +
		"WHERE height <= $1 " +
		"ORDER BY height DESC LIMIT $2"

	var blocks []*BlockRec
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
