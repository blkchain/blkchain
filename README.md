
# Fast Bitcoin Blockchain Postgres Import

## Introduction

This is Go code aimed at importing the blockchain into Postgres as
fast as possible. Licensed under the Apache License, Version 2.0

This code can import the entire blockchain from a Bitcoin Core node
into a PostgreSQL database in 38 hours on fairly basic hardware.

Once the data is imported, the tool can append new blocks as they
appear on the network by connecting to a Core node. You can then run
this primitive (but completely private) [Block Explorer](https://github.com/blkchain/blocks)
against this database.

This project was started mostly for fun and learning and to find out
whether putting the blockchain into PostgreSQL is (1) possible and (2)
useful. We think "yes" on both, but you can draw your own conclusions.

## Quick Overview

The source of the data is the [Bitcoin Core](https://bitcoin.org/en/download) store. You need a Core
instance to download the entire blockchain, then the `cmd/import` tool
will be able to read the data *directly* (not via RPC) by accessing
the LevelDb and the blocks files as well as the UTXO set. (The Core
program cannot run while this happens, but this is only necessary during
the initial bulk import of the data).

You should be able to build `go build cmd/import/import.go` then run
it with (Core should not be running):

```sh
# Warning - this may take many hours
./import \
     -connstr "host=192.168.X.X dbname=blocks sslmode=disable" \
     -cache-size $((1048*1048*75)) \
     -blocks ~/.bitcoin/blocks
```

This will read all blocks and upload them to Postgres. The block
descriptors are first read from leveldb block index, which contains
file names and offsets to actual block data. Using the block index
lets us read blocks in order which is essential for the correct
setting of tx_id in outputs. For every output we also query the
LevelDb UTXO set so that we can set the `spent` column correctly.

The following log excerpt is from an import with the exact parameters
as above, where the sending machine is a 2-core i5 with 16GB of RAM
and the receiving (PostgreSQL server) machine is a 4-core i7 with 16GB
running PostgreSQL version 15, both machines have SSDs. The PostrgeSQL
instance is on a ZFS filesystem.

``` txt
2023/01/12 18:04:04 Tables created without indexes, which are created at the very end.
2023/01/12 18:04:04 Setting table parameters: autovacuum_enabled=false
2023/01/12 18:04:04 Reading block headers from LevelDb (/home/grisha/.bitcoin/blocks/index)...
2023/01/12 18:04:06 Read 768008 block header entries.
2023/01/12 18:04:06 Ignoring orphan block 000000000000000000025edbf5ea025e4af2674b318ba82206f70681d97ca162
... snip ...
2023/01/12 18:04:06 Read 767980 block headers.
2023/01/12 18:04:09 Height: 38776 Txs: 39083 Time: 2010-02-07 01:00:44 -0500 EST Tx/s: 7816.592102 KB/s: 1724.198226 Runtime: 5s
2023/01/12 18:04:14 Height: 75751 Txs: 106727 Time: 2010-08-22 18:54:16 -0400 EDT Tx/s: 10672.579598 KB/s: 2796.868426 Runtime: 10s
2023/01/12 18:04:19 Height: 93070 Txs: 182258 Time: 2010-11-20 23:02:58 -0500 EST Tx/s: 12139.712130 KB/s: 3246.705901 Runtime: 15s
2023/01/12 18:04:24 Height: 105560 Txs: 252852 Time: 2011-01-31 17:37:42 -0500 EST Tx/s: 12633.566623 KB/s: 3472.467068 Runtime: 20s
2023/01/12 18:04:29 Height: 112902 Txs: 319760 Time: 2011-03-09 11:47:10 -0500 EST Tx/s: 12782.638838 KB/s: 3617.003871 Runtime: 25s
2023/01/12 18:04:29 Txid cache hits: 328533 (100.00%) misses: 0 collisions: 0 dupes: 2 evictions: 210438 size: 109320 procmem: 484 MiB
2023/01/12 18:04:34 Height: 116278 Txs: 385424 Time: 2011-04-02 04:59:14 -0400 EDT Tx/s: 12840.690030 KB/s: 3709.375710 Runtime: 30s
... snip ...
2023/01/13 15:02:04 WARNING: Txid cache collision at hash: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 existing id: 713414812 new id: 739931084 (prefix sz: 7).
... snip ...
2023/01/13 16:51:41 Height: 767952 Txs: 789099767 Time: 2022-12-18 11:24:15 -0500 EST Tx/s: 9616.475108 KB/s: 5281.067143 Runtime: 22h47m37s
2023/01/13 16:51:46 Height: 767972 Txs: 789144863 Time: 2022-12-18 15:37:26 -0500 EST Tx/s: 9616.406747 KB/s: 5281.055322 Runtime: 22h47m43s
2023/01/13 16:51:47 Closing channel, waiting for workers to finish...
2023/01/13 16:51:48 Closed db channels, waiting for workers to finish...
2023/01/13 16:51:48 Block writer channel closed, commiting transaction.
2023/01/13 16:51:48 Tx writer channel closed, committing transaction.
2023/01/13 16:51:48 TxOut writer channel closed, committing transaction.
2023/01/13 16:51:48 Block writer done.
2023/01/13 16:51:48 TxOut writer done.
2023/01/13 16:51:48 Tx writer done.
2023/01/13 16:51:48 TxIn writer channel closed, committing transaction.
2023/01/13 16:51:48 TxIn writer done.
2023/01/13 16:51:48 Workers finished.
2023/01/13 16:51:48 Txid cache hits: 2078283772 (99.90%) misses: 2176662 collisions: 1 dupes: 2 evictions: 695537874 size: 82108568 procmem: 8393 MiB
2023/01/13 16:51:48 The following txids collided:
2023/01/13 16:51:48 Txid: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 prefix: c0eb5cf001c74c
2023/01/13 16:51:48 Cleared the cache.
2023/01/13 16:51:48 Creating indexes part 1, please be patient, this may take a long time...
2023/01/13 16:51:48   Starting txins primary key...
2023/01/13 18:38:39   ...done in 1h46m50.12s. Starting txs txid (hash) index...
2023/01/13 19:21:48   ...done in 43m9.592s.
2023/01/13 19:21:48 Running ANALYZE txins, _prevout_miss, txs to ensure the next step selects the optimal plan...
2023/01/13 19:22:39 ...done in 50.436s. Fixing missing prevout_tx_id entries (if needed), this may take a long time..
2023/01/13 19:22:39   max prevoutMiss id: 2176662 parallel: 8
2023/01/13 19:22:39   processing range [1, 10001) of 2176662...
... snip ...
2023/01/13 19:35:12   processing range [2170001, 2180001) of 2176662...
2023/01/13 19:35:28 ...done in 12m49.148s.
2023/01/13 19:35:28 Creating indexes part 2, please be patient, this may take a long time...
2023/01/13 19:35:28   Starting blocks primary key...
2023/01/13 19:35:30   ...done in 2.389s. Starting blocks prevhash index...
2023/01/13 19:35:31   ...done in 1.338s. Starting blocks hash index...
2023/01/13 19:35:32   ...done in 576ms. Starting blocks height index...
2023/01/13 19:35:32   ...done in 302ms. Starting txs primary key...
2023/01/13 19:50:15   ...done in 14m42.608s. Starting block_txs block_id, n primary key...
2023/01/13 20:04:55   ...done in 14m39.797s. Starting block_txs tx_id index...
2023/01/13 20:16:30   ...done in 11m35.015s. Creatng hash_type function...
2023/01/13 20:16:30   ...done in 220ms. Starting txins (prevout_tx_id, prevout_tx_n) index...
2023/01/13 21:34:31   ...done in 1h18m0.887s. Starting txouts primary key...
2023/01/13 22:43:20   ...done in 1h8m49.268s. Starting txouts address prefix index...
2023/01/14 00:30:23   ...done in 1h47m2.445s. Starting txins address prefix index...
2023/01/14 04:00:43   ...done in 3h30m20.279s.
2023/01/14 04:00:43 Creating constraints (if needed), please be patient, this may take a long time...
2023/01/14 04:00:43   Starting block_txs block_id foreign key...
2023/01/14 04:08:47   ...done in 8m3.746s. Starting block_txs tx_id foreign key...
2023/01/14 04:46:29   ...done in 37m42.35s. Starting txins tx_id foreign key...
2023/01/14 06:52:36   ...done in 2h6m7.446s. Starting txouts tx_id foreign key...
2023/01/14 08:15:03   ...done in 1h22m27.124s.
2023/01/14 08:15:03 Creating txins triggers.
2023/01/14 08:15:05 Dropping _prevout_miss table.
2023/01/14 08:15:05 Marking orphan blocks (whole chain)...
2023/01/14 08:15:50 Done marking orphan blocks in 44.698s.
2023/01/14 08:15:50 Reset table storage parameters: autovacuum_enabled.
2023/01/14 08:15:50 Indexes and constraints created.
2023/01/14 08:15:50 All done in 38h11m46.231s.
```

There are two phases to this process, the first is just streaming the
data into Postgres, the second is building indexes, constraints and
otherwise tying up loose ends.

The `-cache-size` parameter is the cache of txid (the SHA256) to the
database tx_id, which import can set on the fly. This cache is also
used to identify duplicate transactions. Having a cache of 80M entries
achieves 99.90% hit rate (as of Dec 2022, see above). The missing ids
will be corrected later, but having as much as possible set from the
beginning will reduce the time it takes to correct them later. A 80M
entry cache will result in the import process taking up ~8GB of RAM.

After the initial import, the tool can "catch up" by importing new
blocks not yet in the database. The catch up is many times slower than
the initial import because it does not have the luxury of not having
indexes and constraints. The catch up does not read LevelDb, it simply
uses the Bitcoin protocol to request new blocks from the node. If you
specify the `-wait` option, the import will wait for new blocks as
they are announced and write them to the DB. For example:

``` sh
# In this example there is a full core node running on 192.168.A.B
# new blocks will be written as they come in.
./import \
    -connstr "host=192.168.X.X dbname=blocks sslmode=disable" \
    -nodeaddr 192.168.A.B:8333 -wait
```

## PostgreSQL Tuning

* Do not underestimate the importance of the sending (client) machine
  performance, it is possible that the client side cannot keep up with
  Postgres. During the initial data load, all that Postgres needs to
  do is stream the incoming tuples to disk. The client needs to parse
  the blocks, format the data for the Postgres writes and maintain a
  cache of the tx_id's. Once the initial load is done, the burden
  shifts unto the server, which needs to build indexes. You can
  specify `-connstr nulldb` to make all database operations noops,
  akin to writing to /dev/null. Try running it this way to see the
  maximum speed you client is capable of before attempting to tune the
  Postgres side.

* Using SSD's on the Postgres server (as well as the sending machine) will
  make this process go much faster. Remember to set `random_page_cost`
  to `1` or less, depending on how fast your disk really is. The
  blockchain will occupy more than 600GB on disk and this will grow as
  time goes on.

* Turning off `synchronous_commit` and setting `commit_delay` to
  `100000` would make the import faster. Turning `fsync` off entirely
  might make it faster even still (heed the documentation warnings).

* `shared_buffers` should not be set high, PostgreSQL does better
  relying on the OS disk buffers cache. Shared buffers are faster than
  OS cache for hits, but more expensive on misses, thus the PG docs
  advise not relying on it unless the whole working set fits in PG
  shared buffers. Of course if your PG server has 512GB of RAM, then
  this advice does not apply.

* Setting `maintenance_work_mem` high should help with speeding up the
  index building. Note that it can be temporarily set right in the
  connection string (`-connstr "host=... maintenance_work_mem=2GB"`).
  Increasing `max_parallel_maintenance_workers` will also help with
  index building. Each worker will get `maintenance_work_mem` divided by
  `max_parallel_maintenance_workers` of memory.

* Setting `wal_writer_delay` to the max value of `10000` and
  increasing `wal_buffers` and `wal_writer_flush_after` should speed
  up the initial import in theory.

* Setting `wal_level` to `minimal` may help as well. (You will also
  need to set `max_wal_senders` to 0 if you use `minimal`).

## ZFS

Using a filesystem which supports snapshots is very useful for
development of this thing because it provides the ability to quickly
rollback to a snapshot should anything go wrong.

ZFS (at least used on a single disk) seems slower than ext4, but still
well worth it. The settings we ended up with are:

``` sh
zfs set compression=zstd-1 tank/blocks # lz4 if your zfs is old
zfs set atime=off tank/blocks
zfs set primarycache=all tank/blocks
zfs set recordsize=16k tank/blocks
zfs set logbias=latency tank/blocks
```

If you use ZFS, then in the Postgres config it is advisable to turn
`full_page_writes`, `wal_init_zero` and `wal_recycle` to `off`.

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
