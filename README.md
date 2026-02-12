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

Try to split instances of this app between big collection and clumping together smaller collections in a single instance.
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

## FAQ

### How performant is this process? 

The sync app is extremely efficient, so much that most of the time is spent waiting for Mongo and writing to s3. 
The overhead of managing records and serializing is negligible. Also, the waiting for Mongo and writing to s3 is separated, 
the sync app can read from Mongo faster than Mongo can send, and write to s3 as fast as s3 can handle.

### What about existing collections?

This is where the `--full-load` flag comes in. The idea is you run sync which starts sending recent data, and then separately run the 
same config with the `--full-load` flag. When this flag is present, the sync app will go through all the collections in the config,
in parallel, and perform a full load. The time this operation takes, depends on how much data Mongo has.

### How many collections per sync process?

This is not something that can be prescribed, and it depends largely on how many processors you have available for the sync app.
As a rule of thumb, if you have 2-3 high volume collections, run those in a single process, and the rest maybe 10-20 smaller collections in another app.

My ideal setup would be 3 instances. One for high volume, and the other two to divide long tail collections, but this largely depends
on how many collections you have. If there's 100 or 200 of them you need to keep in mind that each collection needs attention for checking 
reading and sending. This limit is not on the sync app but more on the OS and network.

### How do I approach full loads in Kubernetes?

You can launch K8S jobs and have them complete. But given that this is a once off thing, the easiest is to start the same mongo2s3 sync 
pod, but instead of running the sync app, just run a while loop in bash. This gives you a shell to ssh into the pod. Once you're in the pod
you can run the sync app manually with full load.

The helm charts provided in this project does this via the `deployment-fullload.yaml` file and you can deploy by setting the `fullload-pod: true`
property in the values file (default false).

Once you're inside the pod, you can use the following command:

```
nohup ./mongo2s3 --config /etc/mongo2s3/config.yaml --full-load &> /work//log.log &
```

### How do I debug?

Set the `debug` property in the values file to true (default false). This works the same as the full load pod mechanism but rather than a separate
pod it starts the main pod and runs a while loop in bash rather than the sync app. This allows you to ssh into the pod and run mongo2s3 manually.

### Can I write to a non S3 destination?

You can write to any s3 compatible api. If you must write to only local disk you can always use a mock s3 api, or launch `https://seaweedfs.com/` in local via docker-compose.
To see how this is done, checkout the `docker-compose.yaml` file of this project.

There are many s3 compatible hosted storage offers out there, AWS (of course), Digital Ocean, Cloudflare and so on.

### How is data partitioned in S3?

The data is partitioned by collection name, followed by the year, month, day and hour the data was received.
This is important, the sync app doesn't use the record timestamp, but the date the data was received from Mongo.

The same goes for a full load. The full load will parition using the date it received each record on.
