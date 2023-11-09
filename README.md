
# Fast Bitcoin Blockchain Postgres Import

## Introduction

This is Go code aimed at importing the blockchain into Postgres as
fast as possible. Licensed under the Apache License, Version 2.0

This code can import the entire blockchain from a Bitcoin Core node
into a PostgreSQL database in 21 hours on fairly basic hardware.

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
     -cache-size 100000000 \
     -blocks ~/.bitcoin/blocks
```

This will read all blocks and upload them to Postgres. The block
descriptors are first read from leveldb block index, which contains
file names and offsets to actual block data. Using the block index
lets us read blocks in order which is essential for the correct
setting of tx_id in outputs. For every output we also query the
LevelDb UTXO set so that we can set the `spent` column correctly.

The following log excerpt is from an import with the exact parameters
as above, where the sending machine is 2019 MacBook Pro with 32GB of
RAM and the receiving (PostgreSQL server) machine is a 4-core i7 with
16GB running PostgreSQL version 15, both machines have SSDs.

``` txt
2023/11/03 16:57:13 Setting open files rlimit of 256 to 1024.
2023/11/03 16:57:13 Tables created without indexes, which are created at the very end.
2023/11/03 16:57:13 Setting table parameters: autovacuum_enabled=false
2023/11/03 16:57:13 Reading block headers from LevelDb (/Users/grisha/Library/Application Support/Bitcoin/blocks/index)...
2023/11/03 16:57:14 Read 814062 block header entries.
2023/11/03 16:57:14 Ignoring orphan block 000000000000000000025edbf5ea025e4af2674b318ba82206f70681d97ca162
2023/11/03 16:57:15 Read 814062 block headers.
2023/11/03 16:57:18 Height: 64469 Txs: 72672 Time: 2010-07-05 15:34:27 -0400 EDT Tx/s: 14534.391593 KB/s: 3574.597873 Runtime: 5s
2023/11/03 16:57:23 Height: 93635 Txs: 191499 Time: 2010-11-24 13:40:43 -0500 EST Tx/s: 19148.955229 KB/s: 5088.048924 Runtime: 10s
2023/11/03 16:57:28 Height: 111512 Txs: 306357 Time: 2011-03-03 05:08:54 -0500 EST Tx/s: 20422.953787 KB/s: 5733.495734 Runtime: 15s
2023/11/03 16:57:33 Height: 118521 Txs: 412626 Time: 2011-04-15 15:15:05 -0400 EDT Tx/s: 20630.450404 KB/s: 6071.899925 Runtime: 20s
2023/11/03 16:57:38 Height: 124440 Txs: 512850 Time: 2011-05-16 17:18:22 -0400 EDT Tx/s: 20510.751291 KB/s: 6473.134698 Runtime: 25s
2023/11/03 16:57:38 Txid cache hits: 645079 (100.00%) misses: 0 collisions: 0 dupes: 2 evictions: 364709 size: 148139 procmem: 434 MiB
2023/11/03 16:57:43 Height: 128385 Txs: 613308 Time: 2011-06-03 12:27:53 -0400 EDT Tx/s: 20438.703244 KB/s: 6757.081103 Runtime: 30s
... snip ...
2023/11/04 03:27:07 WARNING: Txid cache collision at hash: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 existing id: 713414812 new id: 739931084 (prefix sz: 7).
... snip ...
2023/11/04 06:15:13 Height: 813296 Txs: 907828225 Time: 2023-10-22 01:13:19 -0400 EDT Tx/s: 18960.353894 KB/s: 10611.851878 Runtime: 13h18m0s
2023/11/04 06:15:19 Height: 813321 Txs: 907874418 Time: 2023-10-22 06:24:08 -0400 EDT Tx/s: 18959.286326 KB/s: 10611.507696 Runtime: 13h18m6s
2023/11/04 06:15:19 Txid cache hits: 2369583564 (99.91%) misses: 2042901 collisions: 1 dupes: 2 evictions: 777933798 size: 105238549 procmem: 16243 MiB
2023/11/04 06:15:24 Height: 813339 Txs: 907900812 Time: 2023-10-22 09:00:01 -0400 EDT Tx/s: 18957.833769 KB/s: 10610.989629 Runtime: 13h18m11s
2023/11/04 06:15:29 Closing channel, waiting for workers to finish...
2023/11/04 06:15:29 Height: 813369 Txs: 907964339 Time: 2023-10-22 13:56:02 -0400 EDT Tx/s: 18956.918563 KB/s: 10610.608260 Runtime: 13h18m16s
2023/11/04 06:15:30 Closed db channels, waiting for workers to finish...
2023/11/04 06:15:30 Tx writer channel closed, committing transaction.
2023/11/04 06:15:30 Block writer channel closed, commiting transaction.
2023/11/04 06:15:30 TxIn writer channel closed, committing transaction.
2023/11/04 06:15:30 TxOut writer channel closed, committing transaction.
2023/11/04 06:15:30 TxOut writer done.
2023/11/04 06:15:30 Block writer done.
2023/11/04 06:15:30 TxIn writer done.
2023/11/04 06:15:30 Tx writer done.
2023/11/04 06:15:30 Workers finished.
2023/11/04 06:15:30 Txid cache hits: 2369888696 (99.91%) misses: 2056367 collisions: 1 dupes: 2 evictions: 778046986 size: 105221498 procmem: 16243 MiB
2023/11/04 06:15:30 The following txids collided:
2023/11/04 06:15:30 Txid: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 prefix: c0eb5cf001c74c
2023/11/04 06:15:30 Cleared the cache.
2023/11/04 06:15:30 Creating indexes part 1, please be patient, this may take a long time...
2023/11/04 06:15:30   Starting txins primary key...
2023/11/04 07:17:54   ...done in 1h2m24.594s. Starting txs txid (hash) index...
2023/11/04 07:41:41   ...done in 23m47.193s.
2023/11/04 07:41:41 Running ANALYZE txins, _prevout_miss, txs to ensure the next step selects the optimal plan...
2023/11/04 07:42:22 ...done in 40.348s. Fixing missing prevout_tx_id entries (if needed), this may take a long time..
2023/11/04 07:42:22   max prevoutMiss id: 2056367 parallel: 8
2023/11/04 07:42:22   processing range [1, 10001) of 2056367...
2023/11/04 07:42:22   processing range [10001, 20001) of 2056367...
... snip ...
2023/11/04 07:49:32   processing range [2050001, 2060001) of 2056367...
2023/11/04 07:49:39 ...done in 7m17.348s.
2023/11/04 07:49:39 Creating indexes part 2, please be patient, this may take a long time...
2023/11/04 07:49:39   Starting blocks primary key...
2023/11/04 07:49:41   ...done in 1.89s. Starting blocks prevhash index...
2023/11/04 07:49:42   ...done in 718ms. Starting blocks hash index...
2023/11/04 07:49:42   ...done in 695ms. Starting blocks height index...
2023/11/04 07:49:43   ...done in 450ms. Starting txs primary key...
2023/11/04 07:59:06   ...done in 9m23.284s. Starting block_txs block_id, n primary key...
2023/11/04 08:11:27   ...done in 12m20.978s. Starting block_txs tx_id index...
2023/11/04 08:20:20   ...done in 8m53.257s. Creatng hash_type function...
2023/11/04 08:20:20   ...done in 40ms. Starting txins (prevout_tx_id, prevout_tx_n) index...
2023/11/04 09:02:06   ...done in 41m45.629s. Starting txouts primary key...
2023/11/04 09:42:49   ...done in 40m42.816s. Starting txouts address prefix index...
2023/11/04 11:12:41   ...done in 1h29m52.436s. Starting txins address prefix index...
2023/11/04 14:32:54   ...done in 3h20m13.17s.
2023/11/04 14:32:54 Creating constraints (if needed), please be patient, this may take a long time...
2023/11/04 14:32:54   Starting block_txs block_id foreign key...
2023/11/04 14:32:55   ...done in 173ms. Starting block_txs tx_id foreign key...
2023/11/04 14:32:55   ...done in 7ms. Starting txins tx_id foreign key...
2023/11/04 14:32:55   ...done in 6ms. Starting txouts tx_id foreign key...
2023/11/04 14:32:55   ...done in 6ms.
2023/11/04 14:32:55 Creating txins triggers.
2023/11/04 14:32:55 Dropping _prevout_miss table.
2023/11/04 14:32:55 Marking orphan blocks (whole chain)...
2023/11/04 14:33:37 Done marking orphan blocks in 42.163s.
2023/11/04 14:33:37 Reset table storage parameters: autovacuum_enabled.
2023/11/04 14:33:37 Indexes and constraints created.
2023/11/04 14:33:37 All done in 21h36m23.9s.
```

There are two phases to this process, the first is just streaming the
data into Postgres, the second is building indexes, constraints and
otherwise tying up loose ends.

The `-cache-size` parameter is the cache of txid (the SHA256) to the
database tx_id, which import can set on the fly. This cache is also
used to identify duplicate transactions. Having a cache of 100M entries
achieves 99.90% hit rate (as of Nov 2023, see above). The missing ids
will be corrected later, but having as much as possible set from the
beginning will reduce the time it takes to correct them later. A 100M
entry cache will result in the import process taking up ~16GB of RAM.

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
