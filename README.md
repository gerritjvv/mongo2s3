
# TODO

* Uploader
* We need a way to track the files uploaded and the sync tokens. This way we can know what's uploaded to s3.
* We need to cleanout the tracking files.
* If no resume token we should:
 start a change stream 
then start a full scan (if configured to do so)
this ensures new data is uploaded but the full scan will continue in the background till complete.
use coll.Find(ctx, bson.M)
if id is oid like:
 {"age":31,"_id":{"$oid":"6921c82509367aaeff1a792b"},"name":"test"}

we can get the document insertion id using:

objID := doc["_id"].(primitive.ObjectID)
createdAt := objID.Timestamp()
fmt.Println(createdAt)

This means we can full scan and get the original dates.




