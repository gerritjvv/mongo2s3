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

# TODO

* Uploader
* We need a way to track the files uploaded and the sync tokens. This way we can know what's uploaded to s3. <-- done
* We need to cleanout the tracking files.
    * Add DB connection <-- done
* If no resume token we should:
  start a change stream
  then start a full scan (if configured to do so)
  this ensures new data is uploaded but the full scan will continue in the background till complete.
  We need a new table and record here to check the full load

use coll.Find(ctx, bson.M)
if id is oid like:
{"age":31,"_id":{"$oid":"6921c82509367aaeff1a792b"},"name":"test"}

we can get the document insertion id using:

objID := doc["_id"].(primitive.ObjectID)
createdAt := objID.Timestamp()
fmt.Println(createdAt)

This means we can full scan and get the original dates.

The problem is we start receiving new events, and then start the scan we may run into duplicates.
So before uploading from a scan we should check if any resume tokens and if any we need to keep that as a step form whre
beyond we cannot scan.
Or we accept that there may be a slight duplication of records. This should be fine given that the ids can be used to
deduplicate.

* Retry on error. When there are upload errors each runner hsould check for local files and try to upload
*
* We need telemtry here and jaeger support
