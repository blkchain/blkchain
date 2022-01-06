
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
2021/12/28 12:36:41 Setting open files rlimit of 256 to 1024.
2021/12/28 12:36:42 Tables created without indexes, which are created at the very end.
2021/12/28 12:36:42 Reading block headers from LevelDb (/Volumes/SSD/Private/Bitcoin/blocks/index)...
2021/12/28 12:36:46 Read 716166 block header entries.
2021/12/28 12:36:46 Ignoring orphan block 00000000000000000009f819d004fea5bcb77bda25f4906d0a39e79c9ba19590
2021/12/28 12:36:46 Ignoring orphan block 000000000000000000077247c3ca9bae18511418667c4562fc6f92477b5d339e
2021/12/28 12:36:47 Read 716138 block headers.
2021/12/28 12:36:48 Height: 546 Txs: 560 Time: 2009-01-15 01:08:20 -0500 EST Tx/s: 111.862330 KB/s: 23.770744
2021/12/28 12:36:53 Height: 7320 Txs: 7400 Time: 2009-03-13 12:39:49 -0400 EDT Tx/s: 739.526105 KB/s: 159.897534
2021/12/28 12:36:58 Height: 15962 Txs: 16087 Time: 2009-05-29 09:56:14 -0400 EDT Tx/s: 1071.320601 KB/s: 231.352504
2021/12/28 12:37:03 Height: 25261 Txs: 25423 Time: 2009-10-18 19:11:52 -0400 EDT Tx/s: 1270.031636 KB/s: 276.556468

  ... snip ...

2022/01/03 14:32:54 Height: 715863 Txs: 698002803 Time: 2021-12-26 12:34:30 -0500 EST Tx/s: 1328.525074 KB/s: 710.286750
2022/01/03 14:32:59 Height: 715869 Txs: 698010065 Time: 2021-12-26 13:28:25 -0500 EST Tx/s: 1328.525485 KB/s: 710.287700
2022/01/03 14:32:59 Closing channel, waiting for workers to finish...
2022/01/03 14:33:08 Height: 715871 Txs: 698015613 Time: 2021-12-26 14:01:16 -0500 EST Tx/s: 1328.513654 KB/s: 710.281130
2022/01/03 14:33:11 Closed db channels, waiting for workers to finish...
2022/01/03 14:33:11 Block writer channel closed, commiting transaction.
2022/01/03 14:33:11 Tx writer channel closed, committing transaction.
2022/01/03 14:33:11 TxIn writer channel closed, committing transaction.
2022/01/03 14:33:11 Block writer done.
2022/01/03 14:33:11 TxOut writer channel closed, committing transaction.
2022/01/03 14:33:11 TxIn writer done.
2022/01/03 14:33:11 Tx writer done.
2022/01/03 14:33:11 TxOut writer done.
2022/01/03 14:33:11 Workers finished.
2022/01/03 14:33:11 Txid cache hits: 1757524653 (98.39%) misses: 28694337 collisions: 0 dupes: 2 evictions: 595151760
2022/01/03 14:33:11 Creating indexes part 1 (if needed), please be patient, this may take a long time...
2022/01/03 14:33:11   - blocks primary key...
2022/01/03 14:33:12   - blocks prevhash index...
2022/01/03 14:33:13   - blocks hash index...
2022/01/03 14:33:14   - blocks height index...
2022/01/03 14:33:15   - txs primary key...
2022/01/03 14:48:55   - txs txid (hash) index...
2022/01/03 15:23:01   - block_txs block_id, n primary key...
2022/01/03 15:37:55   - block_txs tx_id index...
2022/01/03 15:52:06   - hash_type function...
2022/01/03 15:52:06   - txins (prevout_tx_id, prevout_tx_n) index...
2022/01/03 17:29:35   - txins primary key...
2022/01/03 18:36:52   - txouts primary key...
2022/01/03 19:25:12   - txouts address prefix index...
2022/01/03 22:24:24   - txins address prefix index...
2022/01/04 03:26:27 Creating constraints (if needed), please be patient, this may take a long time...
2022/01/04 03:26:27   - block_txs block_id foreign key...
2022/01/04 03:29:45   - block_txs tx_id foreign key...
2022/01/04 03:35:32   - txins tx_id foreign key...
2022/01/04 04:26:36   - txouts tx_id foreign key...
2022/01/04 04:35:40 Creating txins triggers.
2022/01/04 04:35:40 Fixing missing prevout_tx_id entries (if needed), this may take a long time...
2022/01/04 04:49:02 Dropping _prevout_miss table.
2022/01/04 04:49:02 Marking orphan blocks (whole chain)...
2022/01/04 04:49:13 Done marking orphan blocks.
2022/01/04 04:49:13 Indexes and constraints created.
2022/01/04 04:49:13 All done.
```

There are two phases to this process, the first is just streaming the
data into Postgres, the second is building indexes, constraints and
otherwise tying up loose ends.

The `-cache-size` parameter is the cache of txid (the SHA256) to the
database tx_id, which import can set on the fly. This cache is also
used to identify duplicate transactions. Having a cache of 30M
(default) entries achieves 98.39% hit rate (as of Jan 2022, see
above). The missing ids will be corrected later, but having as much as
possible set from the beginning will reduce the time it takes to
correct them later. A 30M entry cache will result in the import
process taking up ~3GB of RAM.

After the initial import, the tool can "catch up" by importing new
blocks not yet in the database. The catch up is many times slower than
the initial import because it does not have the luxury of not having
indexes and constraints. The catch up does not read LevelDb, it simply
uses the Bitcoin protocol to request new blocks from the node. If you
specify the `-wait` option, the import will wait for new blocks as
they are announced and write them to the DB. For example:

``` sh
# In this example there is a full node running on 192.168.1.224, new blocks will be written as they come in.
./import \
    -connstr "host=192.168.1.223 dbname=blocks sslmode=disable" \
    -nodeaddr 192.168.1.224:8333 -wait
```

## PostgreSQL Tuning

* Using SSD's on the Postgres server (as well as the sending machine) will
  make this process go much faster. Remember to set `random_page_cost`
  to `1` or less, depending on how fast your disk really is. The
  blockchain will occupy mode than 600GB on disk and this will grow as
  time goes on.

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
rollback to a snapshot should anything go wrong.

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
