package db

import (
	"log"
	"runtime"
	"sync"

	"github.com/blkchain/blkchain"
)

// We are avoiding defer Unlock() because rumors are it is slower than
// inline.

// An output can only be spent once. Thus, we can increment a count
// for every output and decrement the count at every check and remove
// the entry when the count reaches 0. This is far from perfect,
// because (1) there can be duplicate spending transactions coming
// from orphaned blocks (which will use up the counter on the first
// hit and cause the entry to get purged - but this is adressed in the
// "recent" cache below) and (2) this cache is of limited size and
// some misses are unavoidable. The idea is to do as much as possible
// here, then correct all the discrepancies once it is all in the
// database.
//
// We cache a uint64, first 48 bits is id, remaining 16 bits is count
// To create it: uint64((id << 16) | uint64(uint16(cnt)))
// To get the id out of it: idcnt >> 16
// To decrement the count: idcnt--
// To get the count: idcnt&0xffff

// Use only the first N bytes to save memory
const HASH_PREFIX_SIZE = 10

const RECENT_RING_SIZE = 1024 * 64

type txIdCache struct {
	*sync.Mutex
	m    map[[HASH_PREFIX_SIZE]byte]*uint64
	sz   int
	cols int
	dups int
	hits int
	miss int
	evic int
	// The following "recent cache" is to not purge the N most recent
	// transactions. This is necessary when we detect known
	// transactions during chain splits. (In other words these
	// transactions stay in this small cache no matter what and are
	// only evicted through aging). This is a small map, which is our
	// cache, and a fixed size slice called ring through which we
	// cycle and this is how we know when an entry is no longer recent
	// and delete it from the recent map.
	recent map[[HASH_PREFIX_SIZE]byte]int64
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
		m:      make(map[[HASH_PREFIX_SIZE]byte]*uint64, alloc),
		sz:     sz,
		recent: make(map[[HASH_PREFIX_SIZE]byte]int64, RECENT_RING_SIZE),
		ring:   make([][HASH_PREFIX_SIZE]byte, RECENT_RING_SIZE),
		ring_n: -1,
	}
}

var zeroHashPrefix [HASH_PREFIX_SIZE]byte

func (c *txIdCache) clear() {
	c.m = make(map[[HASH_PREFIX_SIZE]byte]*uint64)
}

// Returns cached id if it is recent, otherwise -1
func (c *txIdCache) addRing(key [HASH_PREFIX_SIZE]byte, id int64) int64 {
	c.Lock()

	result := int64(-1)

	if hit, ok := c.recent[key]; ok {
		result = hit
		c.dups++
	} else {
		c.recent[key] = id
	}

	// Increment or cycle back to 0
	c.ring_n++
	if c.ring_n == RECENT_RING_SIZE {
		c.ring_n = 0
	}

	// If the next entry is non-zero and there was no hit, delete it
	// from the map because it is no longer recent
	if c.ring[c.ring_n] != zeroHashPrefix && result == -1 {
		delete(c.recent, c.ring[c.ring_n])
	}

	// And now put the new value in this next slot
	c.ring[c.ring_n] = key

	c.Unlock()
	return result
}

func (c *txIdCache) checkSize() {
	// NB: locking is up to caller!
	if len(c.m) == c.sz {
		// remove a random entry
		for k, _ := range c.m {
			delete(c.m, k)
			break
		}
	}
}

func (c *txIdCache) add(hash blkchain.Uint256, id int64, cnt int) int64 {
	var (
		key    [HASH_PREFIX_SIZE]byte
		result int64
	)
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	if recent := c.addRing(key, id); recent != -1 {
		// If this transaction is in the recent ring, it means it has
		// already been added to the big cache, so we forego adding.
		result = recent
	} else {
		// Otherwise add it to the big cache
		c.Lock()

		c.checkSize()

		if hit, ok := c.m[key]; ok {
			// If we got this far, it means that a hash is not recent
			// was a duplicate. This may be a collision, though it is
			// highly improbable.
			c.cols++
			log.Printf("WARNING: Txid possible cache collision at hash: %s", hash)
			result = int64(*hit >> 16)
		} else {
			val := uint64(uint64(id<<16) | uint64(uint16(cnt)))
			c.m[key] = &val
			result = id
		}
		c.Unlock()
	}
	return result
}

// Note that check is destructive, see comment at the top.
func (c *txIdCache) check(hash blkchain.Uint256) *int64 {
	var key [HASH_PREFIX_SIZE]byte
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	c.Lock()
	if idcnt, ok := c.m[key]; ok {
		c.hits++
		*idcnt--
		if (*idcnt)&0xffff == 0 { // && !c.recent[key] {
			c.evic++
			delete(c.m, key)
		}
		c.Unlock()
		result := int64(*idcnt >> 16)
		return &result
	}
	c.miss++
	c.Unlock()
	return nil
}

func (c *txIdCache) reportStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Txid cache hits: %d (%.02f%%) misses: %d collisions: %d dupes: %d evictions: %d size: %d procmem: %d MiB",
		c.hits, float64(c.hits)/(float64(c.hits+c.miss)+0.0001)*100,
		c.miss, c.cols, c.dups, c.evic, len(c.m), m.Sys/1024/1024)
}
