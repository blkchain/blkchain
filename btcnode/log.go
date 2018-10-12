package btcnode

import (
	"log"

	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btclog"
)

// btcsuide uses a different logger, logWriter adapts that logger to
// use the stdandard "log" again.

type logWriter struct{}

func (logWriter) Write(p []byte) (n int, err error) {
	log.Print(string(p[24:])) // strip out timestamp
	return len(p), nil
}

func init() {
	peerLog := btclog.NewBackend(logWriter{}).Logger("PEER")
	peerLog.SetLevel(btclog.LevelInfo)
	peer.UseLogger(peerLog)
}
