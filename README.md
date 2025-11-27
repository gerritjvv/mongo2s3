
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
So before uploading from a scan we should check if any resume tokens and if any we need to keep that as a step form whre beyond we cannot scan.
Or we accept that there may be a slight duplication of records. This should be fine given that the ids can be used to deduplicate.

* Retry on error. When there are upload errors each runner hsould check for local files and try to upload
* 
* We need telemtry here and jaeger support
