package blkchain

import (
	"log"
	"sync"
)

// We are avoiding defer Unlock() because rumors are it is slower than
// inline.

// An output can only be spent once. Thus, we can increment a count
// for every output and decrement the count at every check and remove
// the entry when the count reaches 0. This is far from perfect,
// because (1) there can be duplicate spending transactions coming
// from orphaned blocks (which will use up the counter on the first
// hit and cause the entry to get purged) and (2) this cache is of
// limited size and some misses are unavoidable. The idea is to do as
// much as possible here, then correct all the discrepancies once it
// is all in the database.
type idOutCnt struct {
	id  int64
	cnt uint16
}

// Use only the first N bytes to save memory
const HASH_PREFIX_SIZE = 10

const RECENT_RING_SIZE = 1024 * 16

type txIdCache struct {
	*sync.Mutex
	m    map[[HASH_PREFIX_SIZE]byte]*idOutCnt
	sz   int
	cols int
	dups int
	hits int
	miss int
	evic int
	// The following is to not purge the N most recent
	// transactions. This is necessary when we detect known
	// transactions during chain splits.
	recent map[[HASH_PREFIX_SIZE]byte]bool
	ring   [][HASH_PREFIX_SIZE]byte
	ring_n int
}

func newTxIdCache(sz int) *txIdCache {
	alloc := 1024 * 1024
	if sz < alloc {
		alloc = sz
	}
	return &txIdCache{
		Mutex:  new(sync.Mutex),
		m:      make(map[[HASH_PREFIX_SIZE]byte]*idOutCnt, alloc),
		sz:     sz,
		recent: make(map[[HASH_PREFIX_SIZE]byte]bool, RECENT_RING_SIZE),
		ring:   make([][HASH_PREFIX_SIZE]byte, RECENT_RING_SIZE),
		ring_n: -1,
	}
}

var zeroHashPrefix [HASH_PREFIX_SIZE]byte

func (c *txIdCache) addRing(key [HASH_PREFIX_SIZE]byte) bool {
	c.Lock()

	recent := c.recent[key]

	c.ring_n++
	if c.ring_n == RECENT_RING_SIZE {
		c.ring_n = 0
	}
	if c.ring[c.ring_n] != zeroHashPrefix {
		delete(c.recent, c.ring[c.ring_n])
	}
	c.ring[c.ring_n] = key
	c.recent[key] = true

	if recent {
		c.dups++
	}
	c.Unlock()
	return recent
}

func (c *txIdCache) checkSize() {
	// NB: locking is up to caller!
	if len(c.m) == c.sz {
		// remove a random entry, but not if it is recent
		for k, _ := range c.m {
			if !c.recent[k] {
				delete(c.m, k)
				break
			}
		}
	}
}

func (c *txIdCache) add(hash Uint256, id int64, cnt int) int64 {
	var (
		key    [HASH_PREFIX_SIZE]byte
		result int64
	)
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	if c.addRing(key) { // true if this is a recent transaction
		c.Lock()
		result = c.m[key].id
		c.Unlock()
	} else {
		c.Lock()

		c.checkSize()

		if hit, ok := c.m[key]; ok {
			// A collision, this should never happen!
			c.cols++
			log.Printf("ZZZ collision at hash: %s", hash)
			result = hit.id
		} else {
			c.m[key] = &idOutCnt{id, uint16(cnt)}
			result = id
		}
		c.Unlock()
	}
	return result
}

// Note that check is destructive, see comment at the top.
func (c *txIdCache) check(hash Uint256) *int64 {
	var key [HASH_PREFIX_SIZE]byte
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	c.Lock()
	if idcnt, ok := c.m[key]; ok {
		c.hits++
		idcnt.cnt--
		if idcnt.cnt == 0 { // && !c.recent[key] {
			c.evic++
			delete(c.m, key)
		}
		c.Unlock()
		return &idcnt.id
	}
	c.miss++
	c.Unlock()
	return nil
}
