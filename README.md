# blkchain

This is a pile of Go code loosely aimed at doing blockchain stuff,
this is work-in-progress.

## Importing the blockchain into Postgres

This is the first milestone of this project, indended to work in
tandem with https://github.com/blkchain/pg_blkchain

### Source of blockchain data

Eventually it would be cool to get the blockchain the same way any
other node implementation would, i.e. by pulling it from the network,
but for now, we'll settle for reading the on-dick blocks as they are
stored by Bitcoin Core.

If you are completely inpatient (and patience is absolutely essential
here as we are dealing with a substantial chunk of data), you should
be able to build `go build cmd/import/import.go` then run it with:

```sh
./import -blocks /path/to/blocks
```

This will read all blocks and upload them to Postgres. My postgres is
running on a 2011 thinkpad with a spinning hard drive, and the whole
import takes approximately 12 hours, which is relatively fast,
actually.

More details to follow.
