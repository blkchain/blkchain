
# Blockchain Postgres Import

## Introduction

This is Go code loosely aimed at importing the blockchain into
Postgres as fast as possible.

This is work-in-progress and is subject to a lot of changes. Postgres
import was our first goal and as part of it much blockchain-specific
stuff had to be implemented which has nothing to do with Postgres,
therefore it is likely that this project will be reorganized into more
separate packages.

The goal here is to faciliate blockchain development using Go and
PostgreSQL. If this sounds interesting, please help out by trying this
out, we would very much appreciate any feedback, be it criticism, bug
fixes or ideas.

We are focusing on Bitcoin initially, though this code should be
generally applicable to any Bitcoin copycat out there as well.

## Quick Overview

The source of the data is the Bitcoin Core store. You need a Core
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
indexes and constraints.

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
