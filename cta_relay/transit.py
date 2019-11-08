#!/usr/bin/env python
from pprint import pprint
import os

from cta_relay import s3 

def transit(bucket, gridftp_url, gridftp_path, tempdir, compr_threads, obj, dry_run):
    if obj is None:
        objects = [bucket.Object(o.key) for o in bucket.objects.all() if o.size > 0]
                        # Since we use compression even on empty files, only
                        # objects whose bodies have been "emptied" can have size
    else:
        objects = [bucket.Object(obj)]
    print('Un-downloaded keys:', [o.key for o in objects])

    for obj in objects:
        s3.download(obj, tempdir, compr_threads, dry_run)
        src_url = 'file://' + os.path.join(tempdir, obj.key)
        dst_url = gridftp_url + os.path.join(gridftp_path, obj.key)
        #gridftp.copy(src_url, dst_url)
