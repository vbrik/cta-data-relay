#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
import argparse
import sys
from pprint import pprint
from multiprocessing.pool import ThreadPool, Pool

from cta_relay.gridftp import gfal2_list_dir, gfal2_md5, DT_REG

def bulk_set_metadata(bucket, origin, path, pool_size, dry_run):
    pool = ThreadPool(pool_size)
    uploaded_files = set(o.key for o in bucket.objects.all())

    meta = dict((name, {'size':str(stat.st_size), 'mtime':str(stat.st_mtime)})
                                    for name,stat in gfal2_list_dir(origin + path, DT_REG))
    urls = [origin + os.path.join(path, name)
                                    for name in meta if name not in uploaded_files]
    print('Files uploaded:', len(uploaded_files))
    print('Files found:', len(meta))
    print('Files to process:', len(urls))
    if dry_run:
        return
    for url,md5 in pool.imap_unordered(gfal2_md5, urls):
        name = basename(url)
        meta[name]['md5'] = str(md5)
        obj = bucket.Object(name)
        obj.put(Metadata=meta[name])
        print(name, meta[name])

def show_metadata(bucket, obj=None):
    print(obj)
    if obj:
        o = bucket.Object(obj)
        print(o.key, o.metadata)
    else:
        for so in bucket.objects.all():
            o = bucket.Object(so.key)
            print(o.key, o.metadata)
