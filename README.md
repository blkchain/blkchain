# blkchain

This is a pile of Go code loosely aimed at doing blockchain stuff.

This is work-in-progress and is subject to a lot of changes..

### Importing the blockchain into Postgres

This is the first milestone of this project, indended to work in
tandem with https://github.com/blkchain/pg_blkchain

### Source of blockchain data

Eventually it would be cool to get the blockchain the same way any
other node implementation would, i.e. by pulling it from the network,
but for now, we'll settle for reading the on-disk blocks as they are
stored by Bitcoin Core.

You should be able to build `go build cmd/import/import.go` then run
it with (Core should not be running):

```sh
./import -blocks /path/to/blocks
```

This will read all blocks and upload them to Postgres. On a 2011
thinkpad with a spinning hard drive running Linux, and the whole
import takes approximately 9 hours (~9,000 Tx/s), plus additional
X hours to build the indexes and constraints.

On first run `import` will create the tables, initially without any
indexes. This is as to keep the first import as fast as possible, the
indexes and constraints will get created at the very end. It's
important not to ctrl-C its first run, or else the indexes and
constraints will never be created.

The importer creates a goroutine for each table and sends rows in
parallel via channels. The mechanism by which it is written to
postgres is `COPY`, which is by far the fastest method.

### Catching up

On subsequent runs import will query for the last block hash, and skip
the blocks that are already in the database. This way it is possible
to catch up as more data becomes available.

This was only tested when Core is not running, running it while Core
is writing to disk is probably not a good idea. You can speed up the
skipping of blocks by giving it a `-start-idx` option, which is the
number of the `blk*` file from which the scan starts.

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
the "traditional" integer id and just use the hash as key.

One (less obvious) problem is that transaction ids are not unique
(`00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721`).

Another problem is that the hash is 32 bytes, and that space adds up
quickly, also building an index on a 32 byte BYTEA is slower and the
index is unnecessarily large.

#### Indexing the TX hashes

One way to reduce the weight of the txid index is to only index the
first few (8 seems like a good number) bytes. This is possible with
expression indexes: `CREATE INDEX ON txs(substring(txid for 8))`. The
catch is that we need to remember to always use the substring
expression to look up transactions, but the size and speed gain is
well worth it.

#### TX hash index

Regardless, an index on the TX hash is very useful to look up
transactions by txid (hash). The size of the index can be reduced by
creating an _expression index_ of the first few bytes, but the
downside is that it cannot be used as a foreign key. Postgres
documentation discourages use of _hash indexes_ because of
implementation problems, though they theoretically seem well fit for
this.

#### TX hash endian-ness

Note that there is technically no notion of endian-ness when it comes
to a SHA256 hash which is just an array of bytes, but for whatever
reason in Bitcoin code the hashes are treated as little-endian while
printed as big-endian. So the above-mentioned duplicate TX hash is
recorded in the database as
`21d77ccb4c08386a04ac0196ae10f6a1d2c2a377558ca190f143070000000000`.
This is consistent with on-disk storage in Core and the output of
pgcrypto `digest()`. The disadvantage is that we need to remember to
reverse the hashes when we look them up.
