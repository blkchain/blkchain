
# Fast Bitcoin Blockchain Postgres Import

## Introduction

This is Go code aimed at importing the blockchain into Postgres as
fast as possible. Licensed under the Apache License, Version 2.0

This code can import the entire blockchain from a Bitcoin Core node
into a PostgreSQL database in 39 hours on fairly basic hardware.

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
2023/09/05 11:36:20 Setting open files rlimit of 256 to 1024.
2023/09/05 11:36:20 Tables created without indexes, which are created at the very end.
2023/09/05 11:36:20 Setting table parameters: autovacuum_enabled=false
2023/09/05 11:36:20 Reading block headers from LevelDb (/Users/grisha/Library/Application Support/Bitcoin/blocks/index)...
2023/09/05 11:36:22 Read 804566 block header entries.
2023/09/05 11:36:22 Ignoring orphan block 000000000000000000025edbf5ea025e4af2674b318ba82206f70681d97ca162
2023/09/05 11:36:22 Read 804566 block headers.
2023/09/05 11:36:25 Height: 38636 Txs: 38941 Time: 2010-02-06 07:32:06 -0500 EST Tx/s: 7788.164632 KB/s: 1717.992157 Runtime: 5s
2023/09/05 11:36:30 Height: 76145 Txs: 107402 Time: 2010-08-24 15:20:18 -0400 EDT Tx/s: 10740.109348 KB/s: 2814.876207 Runtime: 10s
2023/09/05 11:36:35 Height: 93064 Txs: 180568 Time: 2010-11-20 22:35:29 -0500 EST Tx/s: 12034.100716 KB/s: 3223.924409 Runtime: 15s
2023/09/05 11:36:40 Height: 105402 Txs: 251712 Time: 2011-01-30 19:15:09 -0500 EST Tx/s: 12582.453957 KB/s: 3453.436500 Runtime: 20s
2023/09/05 11:36:45 Height: 112895 Txs: 319603 Time: 2011-03-09 10:07:55 -0500 EST Tx/s: 12781.112691 KB/s: 3616.509043 Runtime: 25s
2023/09/05 11:36:45 Txid cache hits: 328338 (100.00%) misses: 0 collisions: 0 dupes: 2 evictions: 210383 size: 109218 procmem: 442 MiB
2023/09/05 11:36:50 Height: 116324 Txs: 385888 Time: 2011-04-02 10:04:44 -0400 EDT Tx/s: 12860.039377 KB/s: 3715.497202 Runtime: 30s
... snip ...
2023/09/06 08:42:38 WARNING: Txid cache collision at hash: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 existing id: 713414812 new id: 739931084 (prefix sz: 7).
... snip ...
2023/09/06 13:24:01 Height: 804531 Txs: 883603990 Time: 2023-08-23 10:22:51 -0400 EDT Tx/s: 9515.347690 KB/s: 5318.410806 Runtime: 25h47m41s
2023/09/06 13:24:06 Height: 804546 Txs: 883637536 Time: 2023-08-23 13:33:17 -0400 EDT Tx/s: 9515.192756 KB/s: 5318.373669 Runtime: 25h47m46s
2023/09/06 13:24:11 Closing channel, waiting for workers to finish...
2023/09/06 13:24:11 Height: 804565 Txs: 883679435 Time: 2023-08-23 16:52:21 -0400 EDT Tx/s: 9515.122134 KB/s: 5318.404833 Runtime: 25h47m51s
2023/09/06 13:24:11 Block writer channel closed, commiting transaction.
2023/09/06 13:24:11 Tx writer channel closed, committing transaction.
2023/09/06 13:24:11 Closed db channels, waiting for workers to finish...
2023/09/06 13:24:11 TxOut writer channel closed, committing transaction.
2023/09/06 13:24:11 Block writer done.
2023/09/06 13:24:11 TxIn writer channel closed, committing transaction.
2023/09/06 13:24:11 Tx writer done.
2023/09/06 13:24:11 TxOut writer done.
2023/09/06 13:24:11 TxIn writer done.
2023/09/06 13:24:11 Workers finished.
2023/09/06 13:24:11 Txid cache hits: 2307823354 (99.98%) misses: 407030 collisions: 1 dupes: 2 evictions: 758699927 size: 109819076 procmem: 16559 MiB
2023/09/06 13:24:11 The following txids collided:
2023/09/06 13:24:11 Txid: 0f157800dba58b15ad242b3f7b48b4010079515e2c9e4702384cc701f05cebc0 prefix: c0eb5cf001c74c
2023/09/06 13:24:11 Cleared the cache.
2023/09/06 13:24:11 Creating indexes part 1, please be patient, this may take a long time...
2023/09/06 13:24:11   Starting txins primary key...
2023/09/06 15:26:25   ...done in 2h2m13.162s. Starting txs txid (hash) index...
2023/09/06 16:14:42   ...done in 48m17.008s.
2023/09/06 16:14:42 Running ANALYZE txins, _prevout_miss, txs to ensure the next step selects the optimal plan...
2023/09/06 16:16:11 ...done in 1m29.017s. Fixing missing prevout_tx_id entries (if needed), this may take a long time..
2023/09/06 16:16:11   max prevoutMiss id: 407030 parallel: 8
2023/09/06 16:16:11   processing range [1, 10001) of 407030...
2023/09/06 16:16:11   processing range [20001, 30001) of 407030...
... snip ...
2023/09/06 16:18:55   processing range [400001, 410001) of 407030...
2023/09/06 16:19:12 ...done in 3m0.784s.
2023/09/06 16:19:12 Creating indexes part 2, please be patient, this may take a long time...
2023/09/06 16:19:12   Starting blocks primary key...
2023/09/06 16:19:15   ...done in 3.234s. Starting blocks prevhash index...
2023/09/06 16:19:16   ...done in 822ms. Starting blocks hash index...
2023/09/06 16:19:16   ...done in 846ms. Starting blocks height index...
2023/09/06 16:19:17   ...done in 376ms. Starting txs primary key...
2023/09/06 16:36:51   ...done in 17m33.722s. Starting block_txs block_id, n primary key...
2023/09/06 16:56:29   ...done in 19m38.532s. Starting block_txs tx_id index...
2023/09/06 17:11:46   ...done in 15m16.851s. Creatng hash_type function...
2023/09/06 17:11:46   ...done in 196ms. Starting txins (prevout_tx_id, prevout_tx_n) index...
2023/09/06 18:32:23   ...done in 1h20m37.075s. Starting txouts primary key...
2023/09/06 19:50:53   ...done in 1h18m29.85s. Starting txouts address prefix index...
2023/09/06 22:06:56   ...done in 2h16m3.167s. Starting txins address prefix index...
2023/09/07 02:32:28   ...done in 4h25m31.681s.
2023/09/07 02:32:28 Creating constraints (if needed), please be patient, this may take a long time...
2023/09/07 02:32:28   Starting block_txs block_id foreign key...
2023/09/07 02:32:29   ...done in 701ms. Starting block_txs tx_id foreign key...
2023/09/07 02:32:29   ...done in 45ms. Starting txins tx_id foreign key...
2023/09/07 02:32:29   ...done in 39ms. Starting txouts tx_id foreign key...
2023/09/07 02:32:29   ...done in 26ms.
2023/09/07 02:32:29 Creating txins triggers.
2023/09/07 02:32:29 Dropping _prevout_miss table.
2023/09/07 02:32:29 Marking orphan blocks (whole chain)...
2023/09/07 02:33:41 Done marking orphan blocks in 1m12.006s.
2023/09/07 02:33:41 Reset table storage parameters: autovacuum_enabled.
2023/09/07 02:33:41 Indexes and constraints created.
2023/09/07 02:33:41 All done in 38h57m20.571s.
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
