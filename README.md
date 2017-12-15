# blkchain

This is a pile of Go code loosely aimed at doing blockchain stuff. Our
first objective is to import the blockchain into Postgres.

This is work-in-progress and is subject to a lot of changes.

### Source of blockchain data

Eventually it would be cool to get the blockchain the same way any
other node implementation would, i.e. by pulling it from the network,
but for now, we'll settle for reading the on-disk blocks as they are
stored by Bitcoin Core.

You should be able to build `go build cmd/import/import.go` then run
it with (Core should not be running):

```sh
# Warning - this takes a long time
./import \
     -connstr "host=192.168.1.223 dbname=blocks sslmode=disable" \
     -cache-size $((1048*1048*30)) \
     -blocks /whataver/blocks
```

This will read all blocks and upload them to Postgres. The block
descriptors are first read from leveldb block index, which contains
file names and offsets to actual block data. Using the block index
lets us read blocks in order which is essential for the correct
setting of tx_id in outputs.

There are two phases to this process, the first is just streaming the
data into Postgres, the second is building indexes, verification,
identifying spent outputs, etc. Each part takes hours. On not too
fancy hardware we can stream ~10K transactions per second, the entire
data transfer takes ~8 hours, then the index building phase takes
about as much for a total of ~16 hours.

It helps a great deal if Postgres is well tuned and is running on fast
hardware, especially a fast SSD is going to help a lot in the index
building phase. If you use an SSD, remember to set `random_page_cost`
to 1 or less. Setting `maintenance_work_mem` high should help building
indexes faster. Note that it can be temporarily set right in the
connection string (`-connstr "host=... maintenance_work_mem=2GB"`).

The blockchain will occupy more than 300GB of space in Postgres, and
realistically, including any temporary table operations you need at
least 500GB.

The `-cache-size` parameter is the cache of txid (the SHA256) to the
database tx_id, which import can set on the fly. This cache is also
used to identify duplicate transactions. Having a cache of 30M entries
achieves nearly 100% hit rate. The missing ids will be corrected
later, but having as much as possible set from the beginning will
reduce the time it takes to correct them later. A 30M entry cache will
result in the import process being ~3GB.

The initial data stream is done via `COPY`, with a separate goroutine
streaming to its table. So we read blocks in order, iterate over the
transactions therein, the transactions are split into inputs, outpus,
etc, and each of those records is sent over a channel to the goroutine
responsible for that table. This approach is very performant.

### Catching up

On subsequent runs import will query for the last block hash, and skip
the blocks that are already in the database. This way it is possible
to catch up as more data becomes available.

Since during the catch up all indexes and constraints are in place,
inserting a transaction is much (nearly 10x) slower.

### Data Structure Notes

For posterity, the initial (flawed) idea was to store the entire
transaction as a BYTEA column in a single table. This would be the
most compact method of storing the chain that would mirror how Core
stores it on disk. The transactions could be de-serialized on-the-fly
via a C extension which could then turn it into a human-readable data
structure. Where it fell apart was primarily indexing of this
data. The basic b-tree PG index can only have one index entry
correspond to a single row, but for fast access to outputs we would
need some kind of an index. A GIN index seems appropriate for this
because it can have multiple entries point to the same row, but in
practice building such an index took several days and the index took
up many gigabytes of space.

Plan B was to separate blocks, tranactions, inputs and outputs into
separate tables. This works quite well, though it takes more space. It
is also somewhat involved to piece all the parts into the original
transaction serialization format so that one could obtain the
transaction id (i.e. the hash), but it is doable, more details to
follow.

#### Id vs TX hash

Within the blockchain, transactions are referenced by their hashes,
and that might seem like a natural way to do this, i.d. dispense with
the "traditional" integer id and just use the hash as key. The tricky
problem with that is that the hash depends on the transaction
content. It is also 32 bytes long, which is relatively big and less
performant when indexing. Therefore we use a `BIGINT` id for
transactions.

It is not unusual for transactions to be included in more than one
block during chain splits, in fact
`e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468`
coinbase is actually used in two blocks, and this does not break the
rules.  Transaction hashes are enforced unique, a transaction included
in multiple blocks is still the same transaction.

#### TX hash endian-ness

Note that there is technically no notion of endian-ness when it comes
to a SHA256 hash which is just an array of bytes, but for whatever
reason in Bitcoin code the hashes are treated as little-endian while
printed as big-endian. So the above-mentioned duplicate TX hash is
recorded in the database as
`68b45f58b674e94eb881cd67b04c2cba07fe5552dbf1d5385637b0d4073dbfe3`
This is consistent with on-disk storage in Core and the output of
pgcrypto `digest()`. The disadvantage is that we need to remember to
reverse the hashes when we look them up.
