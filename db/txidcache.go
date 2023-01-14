package db

import (
	"log"
	"runtime"
	"sync"

	"github.com/blkchain/blkchain"
)

// Even though this cache uses a prefix of the txid as the key, it is
// collision-proof. This is because txids are arriving (via the add()
// call) in temporal order, and a txin can only refer to txids from
// past blocks. If we add a txid resulting in a collision it is at
// that point deleted from the cache resulting in a miss.
//
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
//  * Create it: uint64((id << 16) | uint64(uint16(cnt)))
//  * Get the id out of it: idcnt >> 16
//  * Decrement the count: idcnt--
//  * Get the txouts count: idcnt&0xffff

const (
	HASH_PREFIX_SIZE = 7 // 3 collisions as of height 771643
	RECENT_RING_SIZE = 1024 * 64
)

type txIdCache struct {
	*sync.Mutex
	m    map[[HASH_PREFIX_SIZE]byte]uint64
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
	// and delete it from the recent map. The recent ring MUST use
	// full-size txid as key because of otherwise (highly improbable)
	// possibility of a txid prefix collision in the recent ring.
	recent map[blkchain.Uint256]int64
	ring   []blkchain.Uint256
	ring_n int

	// useful for debugging
	collisions map[blkchain.Uint256][HASH_PREFIX_SIZE]byte
}

func newTxIdCache(sz int) *txIdCache {
	alloc := 1024 * 1024
	if sz < alloc {
		alloc = sz
	}
	return &txIdCache{
		Mutex:      new(sync.Mutex),
		m:          make(map[[HASH_PREFIX_SIZE]byte]uint64, alloc),
		sz:         sz,
		recent:     make(map[blkchain.Uint256]int64, RECENT_RING_SIZE),
		ring:       make([]blkchain.Uint256, RECENT_RING_SIZE),
		ring_n:     -1,
		collisions: make(map[blkchain.Uint256][HASH_PREFIX_SIZE]byte),
	}
}

var (
	zeroHashPrefix [HASH_PREFIX_SIZE]byte
	zeroUint256    blkchain.Uint256
)

func (c *txIdCache) clear() {
	c.m = make(map[[HASH_PREFIX_SIZE]byte]uint64)
}

// Returns cached id if it is recent, otherwise -1
func (c *txIdCache) addRing(key blkchain.Uint256, id int64) int64 {
	// We are avoiding defer Unlock() because it is rumored to be
	// slower than inline.
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
	if c.ring[c.ring_n] != zeroUint256 && result == -1 {
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
	copy(key[:], hash[:HASH_PREFIX_SIZE]) // because map keys cannot be slices

	if recent := c.addRing(hash, id); recent != -1 {
		// If this transaction is in the recent ring, it means it has
		// already been added to the big cache, so we forego adding.
		result = recent
	} else {
		// Otherwise add it to the big cache
		c.Lock()

		c.checkSize()

		if hit, ok := c.m[key]; ok {
			// If we got this far, it means that a hash that is not
			// recent was a duplicate. This is a discovery of a
			// collision (highly improbable unless txid is the infamous dupe
			// d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599).
			// Delete it from the cache.
			c.collisions[hash] = key
			delete(c.m, key)
			c.cols++
			result = int64(hit >> 16)
			log.Printf("WARNING: Txid cache collision at hash: %s existing id: %d new id: %d (prefix sz: %d).", hash, result, id, HASH_PREFIX_SIZE)
		} else {
			c.m[key] = uint64(uint64(id<<16) | uint64(uint16(cnt)))
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
		idcnt--
		if idcnt&0xffff == 0 { // && !c.recent[key] {
			c.evic++
			delete(c.m, key)
		} else {
			c.m[key] = idcnt
		}
		c.Unlock()
		result := int64(idcnt >> 16)
		return &result
	}
	c.miss++
	c.Unlock()
	return nil
}

func (c *txIdCache) reportStats(cols bool) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m) // they say m.Sys should be close to RSS
	log.Printf("Txid cache hits: %d (%.02f%%) misses: %d collisions: %d dupes: %d evictions: %d size: %d procmem: %d MiB",
		c.hits, float64(c.hits)/(float64(c.hits+c.miss)+0.0001)*100,
		c.miss, c.cols, c.dups, c.evic, len(c.m), m.Sys/1024/1024)
	if cols && len(c.collisions) > 0 {
		log.Printf("The following txids collided:")
		for k, v := range c.collisions {
			log.Printf("Txid: %s prefix: %x", k, v)
		}
	}
}
