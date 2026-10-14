# Chapter 6: What the Guarantees Promise, and Where They Stop

This directory reproduces both the duplicate from FLINK-38450 and the fix on a laptop. Docker Compose runs MySQL, a Flink cluster with Flink CDC, and Spark SQL for reading the Iceberg table, which lives on a local Docker volume. The duplicate needs a schema change in the middle of a checkpoint, which Example 6-5 makes with an ADD COLUMN. The sink handles schema change events the same way whichever source sends them, and the MySQL source sends them by default.

## What you need

- Docker with the Compose plugin (`docker compose`). The run script also uses bash, curl, and python3.
- 4 GB of memory for Docker (the stack ran within a 4 GB limit with no swap), about 9 GB of disk for the images and build cache, and a network connection for the first build, which downloads the base images and the JARs.
- About ten minutes per version once the images are built. Most of it is waiting for two Flink checkpoints.

It was tested on macOS on Apple Silicon (Docker Desktop, Docker Engine 29.3.1). On the same Mac, it also ran inside a Linux arm64 Docker-in-Docker container limited to 4 GB of memory, and with x86_64 images under emulation. On Windows, use a WSL 2 terminal with the repository cloned inside WSL, since the commands below are bash syntax; this was not tested on Windows.

## Try it step by step

Run the commands from this directory. Start the stack with the release from before the fix.

```bash
FLINK_CDC_VERSION=3.6.0 docker compose up -d
```

The variable picks which release `up` builds and starts. The `exec`, `logs`, and `down` commands below work without it, but running `up` again without it switches the stack back to 3.6.0.

The pipeline definition in [`customers-to-iceberg.yaml`](customers-to-iceberg.yaml) (Example 6-4) reads one table and writes it to Iceberg. The Flink configuration in [`docker-compose.yml`](docker-compose.yml) sets the checkpoint interval to five minutes, so the changes you make next will all land inside one checkpoint.

Submit the pipeline with the Flink CDC command-line tool. The image already includes the extra JARs that the MySQL source and a Hadoop catalog need, and sets `FLINK_HOME`. The password in the pipeline definition is the local one the Compose file sets, since `flink-cdc.sh` does not expand environment variables in the pipeline definition.

```bash
docker compose exec jobmanager ./bin/flink-cdc.sh customers-to-iceberg.yaml
```

Wait for a checkpoint to complete. Flink starts the first one at a random point up to five minutes after the job starts, then one every five minutes. To watch, open http://localhost:8081, then Running Jobs, `customers-to-iceberg`, and the Checkpoints tab, or run this and press Ctrl-C when a line appears:

```bash
docker compose logs -f jobmanager | grep "Completed checkpoint"
```

Then make the morning's changes, all within the same checkpoint interval, as in Example 6-5.

```bash
docker compose exec -T mysql mysql -uroot -pcdc-demo < sql/example-6-5.sql
```

Wait for the next checkpoint to complete, then run Example 6-1 against the Iceberg table, then Example 6-2. Spark prints several WARN lines before each result, such as `Unable to load native-hadoop library` and, on the first query, `NoSuchObjectException` for the `default` database. They are expected.

```bash
docker compose exec spark /opt/spark/bin/spark-sql -f /opt/ch06/sql/example-6-1.sql
docker compose exec spark /opt/spark/bin/spark-sql -f /opt/ch06/sql/example-6-2.sql
```

Customer 4412 comes back with two live rows, and Example 6-2 shows both data files and both equality deletes at one sequence number. To see the rows themselves, and which file each is in:

```bash
docker compose exec spark /opt/spark/bin/spark-sql -f /opt/ch06/sql/evidence/table.sql
```

On 3.6.0 it shows two rows for 4412, trial and enterprise. If Example 6-2 shows no rows, the commit for that checkpoint hasn't happened yet; wait a few seconds and run the queries again.

Now stop the stack, start it again with `FLINK_CDC_VERSION=3.7.0`, and repeat the same steps on a fresh table. The `-v` removes the warehouse volume, so the table starts empty.

```bash
docker compose down -v
FLINK_CDC_VERSION=3.7.0 docker compose up -d
```

This time Example 6-1 comes back empty. Example 6-3 shows two snapshots for the checkpoint, batch 0 and batch 1, and customer 4412 is on the enterprise plan. `evidence/table.sql` shows it as a single enterprise row.

```bash
docker compose exec spark /opt/spark/bin/spark-sql -f /opt/ch06/sql/example-6-3.sql
```

## Clean up

```bash
docker compose down -v
docker image rm eosw-ch06-flink:3.6.0 eosw-ch06-flink:3.7.0 eosw-ch06-spark:3.5.8-iceberg-1.10.1
```

The MySQL image and Docker's build cache stay behind. To remove them too:

```bash
docker image rm mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b
docker builder prune
```

`docker builder prune` is not limited to this project, so it can also remove build cache that other projects use.

## Or run it all at once

```bash
./run.sh 3.6.0
./run.sh 3.7.0
```

Each run starts from a fresh stack, submits the pipeline, waits for a checkpoint through the Flink REST API, runs Example 6-5 right away, waits for the next checkpoint and its Iceberg commit, then runs Examples 6-1, 6-2, and 6-3. It prints what Example 6-1 returned and where the results are. Every wait has a timeout, so a failed job stops the script with a message instead of hanging. Set `FLINK_UI_PORT` if port 8081 is taken.

It saves the outputs of Examples 6-1, 6-2, 6-3, and 6-5 to `results/<version>/`, replacing the outputs committed in this repository; `git diff` compares a run with them, and `git checkout results` restores them. The logs and other raw output go to `run-logs/<version>/`, which git ignores.

## What's here

| Path | What it is |
| --- | --- |
| `docker-compose.yml` | MySQL 8.0.46, Flink 1.20.3 (JobManager and TaskManager), Spark 3.5.8, and the shared `/opt/warehouse` volume |
| `flink/Dockerfile` | Flink with the Flink CDC distribution, its MySQL and Iceberg pipeline connectors, the MySQL JDBC driver, and the Hadoop JAR, each checked against a pinned SHA-512 |
| `spark/` | Spark SQL with the Iceberg 1.10.1 runtime and a Hadoop catalog named `lake` |
| `mysql/init.sql` | The `crm.customers` table, which fills `updated_at` on every insert and update |
| `customers-to-iceberg.yaml` | Example 6-4 |
| `sql/example-6-*.sql` | Examples 6-1, 6-2, 6-3, and 6-5, as printed in the chapter |
| `sql/evidence/table.sql` | Shows each row of the table and the data file it is in |
| `run.sh` | The whole walkthrough for one version |
| `results/` | The outputs of Examples 6-1, 6-2, 6-3, and 6-5 from a run of each version |
| `run-logs/` | Created by `run.sh`: the logs and other raw output of each run, ignored by git |
