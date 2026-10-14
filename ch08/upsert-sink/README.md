# An upsert keeps the last write it receives

An upsert sink applies each change as it arrives. If two versions of a key arrive out of order, the plain upsert keeps whichever arrived last. The same upsert with a version check on write keeps the newer version.

## What the run does

PostgreSQL 18.6 with pgvector 0.8.7. The table is what an agent's retrieval reads: one row per customer, with an embedding. Customer 4412's update (source version 2, `enterprise`) arrives first, then the insert (source version 1, `trial`) arrives late, as it can after a retry, a second writer, or a repartition.

- `sql/plain-upsert.sql` applies both with `INSERT ... ON CONFLICT DO UPDATE`.
- `sql/versioned-upsert.sql` applies both with the same statement plus `WHERE customer_docs.source_version < EXCLUDED.source_version`.

Each ends with the stored row and a nearest-neighbor lookup, as a retrieval step would run it.

## Result (`results/`)

- Plain upsert: the late insert is applied (`INSERT 0 1`), and the row and the vector lookup both return `trial`, source version 1 (`plain-upsert.txt`).
- Version check on write: the late insert is skipped (`INSERT 0 0`), and the row and the lookup both return `enterprise`, source version 2 (`versioned-upsert.txt`).

The version check works only if each change carries a version that follows the source's order, here `source_version`.

## Where this is in PostgreSQL's documentation (18)

- `ON CONFLICT DO UPDATE` and its optional `WHERE` condition, which leaves the existing row alone when false: <https://www.postgresql.org/docs/18/sql-insert.html#SQL-ON-CONFLICT>.
- pgvector stores the embedding in an ordinary column, so the same statement applies to it: <https://github.com/pgvector/pgvector/tree/v0.8.7>.

## Run it

```bash
./run.sh
```

It needs Docker with the Compose plugin, takes a few seconds once the image is pulled, and removes its container and volume at the end.
