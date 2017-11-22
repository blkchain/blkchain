package blkchain

import "sync"

// We are avoiding defer Unlock() because rumors are it is slower than
// inline.

// An output can only be spent once. Thus, we can decrement the count
// at every check and remove the entry when the count reaches 0
type idOutCnt struct {
	id  int64
	cnt uint16
}

// Use only the first N bytes to save memory
const HASH_PREFIX_SIZE = 8

type txIdCache struct {
	*sync.Mutex
	m    map[[HASH_PREFIX_SIZE]byte]*idOutCnt
	sz   int
	cols int
	hits int
	miss int
	evic int
}

func newTxIdCache(sz int) *txIdCache {
	alloc := 1024 * 1024
	if sz < alloc {
		alloc = sz
	}
	return &txIdCache{
		Mutex: new(sync.Mutex),
		m:     make(map[[HASH_PREFIX_SIZE]byte]*idOutCnt, alloc),
		sz:    sz,
	}
}

func (c *txIdCache) add(hash Uint256, id int64, cnt int) {
	// check size
	if len(c.m) == c.sz {
		c.Lock()
		// remove a random entry
		for k, _ := range c.m {
			delete(c.m, k)
			break
		}
		c.Unlock()
	}

	var key [HASH_PREFIX_SIZE]byte
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	c.Lock()
	if _, ok := c.m[key]; ok {
		c.cols++
	} else {
		c.m[key] = &idOutCnt{id, uint16(cnt)}
	}
	c.Unlock()
}

// Note that check is destructive
func (c *txIdCache) check(hash Uint256) *int64 {
	var key [HASH_PREFIX_SIZE]byte
	copy(key[:], hash[:HASH_PREFIX_SIZE])

	c.Lock()
	if idcnt, ok := c.m[key]; ok {
		c.hits++
		idcnt.cnt--
		if idcnt.cnt == 0 {
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
