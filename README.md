
# Blockchain Postgres Import

## Introduction

This is Go code loosely aimed at importing the blockchain into
Postgres as fast as possible. Licensed under the Apache License,
Version 2.0

This is work-in-progress and is subject to a lot of changes. Postgres
import was our first goal and as part of it much blockchain-specific
stuff had to be implemented which has nothing to do with Postgres,
therefore it is likely that this project will be reorganized into more
separate packages.

The goal here is to facilitate blockchain development using Go and
PostgreSQL. There is also a sister project [pg_blockchain](https://github.com/blkchain/pg_blkchain)
which is a Postgres C language extension aimed at doing blockchain stuff inside the server.
If this sounds interesting, please help us out by trying this
out, we would very much appreciate any feedback, be it criticism, bug
fixes or ideas.

We are focusing on Bitcoin initially, though this code should be
generally applicable to any Bitcoin copycat out there as well.

## Quick Overview

The source of the data is the [Bitcoin Core](https://bitcoin.org/en/download) store. You need a Core
instance to download the entire blockchain, then the `cmd/import` tool
will be able to read the data *directly* (not via RPC) by accessing
the LevelDb and the blocks files as well as the UTXO set. (The Core
program cannot run while this happens).

You should be able to build `go build cmd/import/import.go` then run
it with (Core should not be running):

```sh
# Warning - this may take 12 hours
./import \
     -connstr "host=192.168.1.223 dbname=blocks sslmode=disable" \
     -blocks /whataver/blocks
```

This will read all blocks and upload them to Postgres. The block
descriptors are first read from leveldb block index, which contains
file names and offsets to actual block data. Using the block index
lets us read blocks in order which is essential for the correct
setting of tx_id in outputs. For every output we also query the UTXO
set so that we can set the `spent` column correctly.

The output should look approximately like this:

``` txt
2017/12/19 18:24:42 Setting open files rlimit of 256 to 1024.
2017/12/19 18:24:42 Reading block headers from LevelDb (/Volumes/SSD/Private/Bitcoin/blocks/index)...
2017/12/19 18:24:46 Ignoring orphan block 00000000000000000b5f97a18352ec38f9b5b20603c8fcd8f25ec9ee1ae0cf93
2017/12/19 18:24:46 Ignoring orphan block 0000000000000000014c463a40271798691071aba2c0d680403031bfdbcf0da3
2017/12/19 18:24:46 Ignoring orphan block 00000000000000000b945410c95718f36319521f6c22822a0032a813955a8198
2017/12/19 18:24:46 Ignoring orphan block 0000000000000000062459292110b290ae1312db9a3ca668b0c187181f6f27f3
2017/12/19 18:24:46 Ignoring orphan block 0000000000000000071412f6831a0f03b5abfb7490e8a4dfa13c82d6805096f1
2017/12/19 18:24:46 Ignoring orphan block 00000000000001e7f5686eedcf6fe2d27c7892b17c8b032e9b5c9241a9d8de32
2017/12/19 18:24:46 Read 499872 block headers.
2017/12/19 18:24:53 Height: 36276 Txs: 36525 Time: 2010-01-25 07:02:08 -0500 EST Tx/s: 7291.832654
2017/12/19 18:24:58 Height: 66780 Txs: 77685 Time: 2010-07-13 15:08:39 -0400 EDT Tx/s: 7761.014587
2017/12/19 18:25:03 Height: 86063 Txs: 130220 Time: 2010-10-18 12:37:57 -0400 EDT Tx/s: 8675.402340
2017/12/19 18:25:08 Height: 97697 Txs: 208377 Time: 2010-12-15 10:19:17 -0500 EST Tx/s: 10413.251706

  ... snip ...

2017/12/20 04:07:02 Height: 499853 Txs: 283295062 Time: 2017-12-17 17:10:19 -0500 EST Tx/s: 8108.974619
2017/12/20 04:07:06 Closed db channels, waiting for workers to finish...
2017/12/20 04:07:06 Block writer channel closed, leaving.
2017/12/20 04:07:06 TxOut writer channel closed, leaving.
2017/12/20 04:07:06 Tx writer channel closed, leaving.
2017/12/20 04:07:06 TxIn writer channel closed, leaving.
2017/12/20 04:07:06 Workers finished.
2017/12/20 04:07:06 Txid cache hits: 704485398 (100.00%) misses: 0 collisions: 0 dupes: 2 evictions: 253053982
2017/12/20 04:07:06 Creating indexes part 1 (if needed), please be patient, this may take a long time...
2017/12/20 04:07:06   - blocks primary key...
2017/12/20 04:07:07   - blocks prevhash index...
2017/12/20 04:07:08   - blocks hash index...
2017/12/20 04:07:09   - blocks height index...
2017/12/20 04:07:10   - txs primary key...
2017/12/20 04:22:19   - txs txid (hash) index...
2017/12/20 04:41:21   - block_txs block_id, n primary key...
2017/12/20 04:51:23   - block_txs tx_id index...
2017/12/20 04:58:07 NOT fixing missing prevout_tx_id entries because there were 0 cache misses.
2017/12/20 04:58:07 Creating indexes part 2 (if needed), please be patient, this may take a long time...
2017/12/20 04:58:07   - txins (prevout_tx_id, prevout_tx_n) index...
2017/12/20 06:32:35   - txins primary key...
2017/12/20 07:04:19   - txouts primary key...
2017/12/20 07:43:47 Creating constraints (if needed), please be patient, this may take a long time...
2017/12/20 07:43:47   - block_txs block_id foreign key...
2017/12/20 07:45:23   - block_txs tx_id foreign key...
2017/12/20 07:52:04   - txins tx_id foreign key...
2017/12/20 09:00:14   - txouts tx_id foreign key...
2017/12/20 10:20:47 Creating txins triggers.
2017/12/20 10:20:47 Marking orphan blocks...
2017/12/20 10:21:41 Indexes and constraints created.
2017/12/20 10:21:41 All done.

```

There are two phases to this process, the first is just streaming the
data into Postgres, the second is building indexes, constraints and
otherwise tying up loose ends.

The `-cache-size` parameter is the cache of txid (the SHA256) to the
database tx_id, which import can set on the fly. This cache is also
used to identify duplicate transactions. Having a cache of 30M
(default) entries achieves 100% hit rate. The missing ids will be
corrected later, but having as much as possible set from the beginning
will reduce the time it takes to correct them later. A 30M entry cache
will result in the import process taking up ~3GB of RAM.

After the initial import, the tool can "catch up" by importing new
blocks not yet in the database. The catch up is many times slower than
the initial import because it does not have the luxury of not having
indexes and constraints. The catch up does not read LevelDb, it simply
uses the Bitcoin protocol to request new blocks from the node. If you
specify the `-wait` option, the import will wait for new blocks as
they are announced and write them to the DB.

## PostgreSQL Tuning

* Using SSD's will make this process go much faster. Remember to set
  `random_page_cost` to `1` or less, depending on how fast your disk
  really is. The blockchain will occupy more than 250GB on disk and
  this will grow as time goes on.

* Turning off `synchronous_commit` and setting `commit_delay` to
  `100000` would make the import faster. Turning `fsync` off entirely
  might make it faster even still (heed the documentation warnings).

* `shared_buffers` should *not* be set high, PostgreSQL does better
  relying on the OS disk buffers cache. Shared buffers are faster than
  OS cache for hits, but more expensive on misses, thus the PG docs
  advise not relying on it unless the whole working set fits in PG
  shared buffers. Of course if your PG server has 512GB of RAM, then
  this advice does not apply.

* Setting `maintenance_work_mem` high should help with speeding up the
  index building. Note that it can be temporarily set right in the
  connection string (`-connstr "host=... maintenance_work_mem=2GB"`).

* Setting `wal_writer_delay` to the max value of `10000` and
  increasing `wal_buffers` and `wal_writer_flush_after` should speed
  up the initial import in theory.

## ZFS

Using a filesystem which supports snapshots is very useful for
development of this thing because it provides the ability to quickly
jump to a snapshot.

ZFS (at least used on a single disk) seems slower than ext4, but still
well worth it. The settings we ended up with are:

``` sh
zfs set compression=lz4 tank/blocks
zfs set atime=off tank/blocks
zfs set primarycache=all tank/blocks # yes, all, not metadata
zfs set recordsize=16k tank/blocks
zfs set logbias=throughput tank/blocks
echo 1 > /sys/module/zfs/parameters/zfs_txg_timeout

```

If you use ZFS, then in the Postgres config it is advisable to turn
`full_page_writes` off.

## Internals of the Data Stream

The initial data stream is done via `COPY`, with a separate goroutine
streaming to its table. We read blocks in order, iterate over the
transactions therein, the transactions are split into inputs, outpus,
etc, and each of those records is sent over a channel to the goroutine
responsible for that table. This approach is very performant.

On catch up the process is slightly more complicated because we need
to ensure that referential integrity is maintained. Each block should
be followed by a commit, all outputs in a block must be commited
before inputs.
