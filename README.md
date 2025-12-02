# Overview

This tool copies inserts and updates from a MongoDB into any S3 compatible storage.
It's quick and kept purposefully simple as can be. The design is to get the raw data into S3, and once there, other
datawarehouse tools and queries
can be used to deduplicate and parse the data into for example parquet or iceberg.

No assumption is made about the collection schemas. Data is written as JSON via the `bson` package and each record is
written on a new line.
For updates, the full document is rewritten, not just the changes. This works best in a datawarehouse where most of the
time you want the latest document
and can get that by doing a distinct on the id object.

Important: This tool does not deduplicate, it copies data faithfully and is designed to avoid data loss.

# Usage

This app can be run in two modes, full load, or cdc sync.
CDC sync is the default, and run full load, you should pass in the --full-load flag.

When starting sync for the first time, run the CDC mode, and then run the full load modes separately once.

## Docker Images

Each merge to the master branch builds out docker images. These are hosted by github.
Check out this page: https://github.com/gerritjvv/mongo2s3/pkgs/container/mongo2s3

If you want the binaries then run `./build.sh build` or peek at the build file and copy and run the individual command for your platform.
# Deploy and Requirements

This app requires a postgres database for tracking the files and of course a MongoDB slave to pull against. Do not
connect it to a MongoDB master, the Mongo CDC will not work that way.
It also requires access to a S3 compatible storage, that supports `PutObject`.

# Configuration

The app is yaml file config driven. If you're in a container environment like Kubernetes, you'll need to mount in the
configmap.

Here's an example config, I use for testing:

```
db_url: "postgres://myuser:mypass@postgres:5432/mydb?sslmode=disable"
mongo_url: "mongodb://mongodb.mongo2s3.orb.local:27017"
local_base_dir: "/tmp/"

s3_bucket: "test"
s3_region: "us-east-1"
s3_prefix: "/data"

file_write_timeout_second: "10s"
file_write_size_bytes: 1048576

collections:
  - db: "test"
    name: "users"
  - db: "test"
    name: "test"
```

Try to split instances of this app between big collectio and clumping together smaller collections in a single instance.
If you have highvolume very time sensitive collections put each in their own instance (Pod, Task).

A file write timeout of 1-5 minutes and a file size bytes of 128-256mb are normally good values for raw ingestion files.
You want to strike a balance between lag for data into your datawarehouse and the number of files.

S3 configuration is configured using the standard AWS S3 environment variables. For exmaple I needed to provide the
values below when connecting to seaweed-fs:

```
S3_ENDPOINT: "http://seaweed-s3:8333"
S3_ACCESS_KEY: "local-access-key"
S3_SECRET_KEY: "local-secret-key"
S3_REGION: "us-east-1"
AWS_EC2_METADATA_DISABLED: "true"
```

# Development

Using docker-compose is the best way to check and test.
It gives you a mongo db, postgres, seaweed fs s3 compatibel cluster all in one.
You'll need to start this app in docker-compose—alreay configured—and it'll have access to all the components.

I start my docker-compose elements in different terminal windows. This helps with debugging and also allows you to
restart the mongo2s3 sync on updates.

* Start mongo and postgres

```bash
docker-compose up --build postgres mongodb
```

* Start the seaweed cluster

```bash
docker-compose up --build seaweed-filer seaweed-volume seaweed-master
```

* Start the seaweedfs s3

```bash
docker-compose up --build seaweed-s3
```

* Start the mongo2s3 sync

```bash
docker-compose up --build --force-recreate mongo2s3
```

* Run a full load

```bash
docker-compose up --build --force-recreate mongo2s3fullload
```

# Sync Semantics

## Sync from CDC

This program uses CDC mongo mechanism via `watch`. It reads data continuousely (ignoring deletes) and sends them to a
writer channel.
The writer channel will accumulate data locally as json/gzip till either the file write timeout has been reached since
the file was created, or the file size is over the threshold.
Once one of these conditions are true, the file is renamed and pushed to an s3 compatible storage.

Files are uploaded using a configured bucket and prefix, and a year, moth, day, hour path partition. For example, '
data/year=2025/month=11/day=25/hour=22/file_1235.gz'.
The date for the partition reflects when the file was uploaded and not the actual database record date.

Note: The file size threshold will never be exact as gzip uses internal buffers and we can only see the full file size
when these bytes are flushed.

## Sync Checkpointing

I used a database to keep track of every file that was uploaded, which includes the first and last mongo id, and the
start and resume cdc tokens.
When the application is restarted the last resume cdc token is read for a db and collection, and passed to the mongo
CDC.

## Error files

It's not if errors happen but when. If a fatal crash, this app will continue from the last CDC resume token for a db and
collection.
If the file was written but could not be uploaded, the CDC mecanism will continue without issues, happily reading and
writing to disk.

By default, there is a routine that checks preriodically for files in the track db that could not be uploaded. These are
marked with 'status=error'.
When they are found, the app will use the start and end mongo ids in the db to re-read that slice of data.

Some improvement can be made here to check if the local file exists, but at the moment of writing this is not a priority
and data is recovered, albeit slower.

