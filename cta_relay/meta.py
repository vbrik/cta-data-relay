import sys
from pprint import pprint
from multiprocessing.pool import ThreadPool
import os
from os.path import basename

def get_gridftp_meta(origin, path, pool_size, exclude=[]):
    from cta_relay import gridftp
    from cta_relay.gridftp import DT_REG
    pool = ThreadPool(pool_size)
    meta = dict((name, {'size':str(stat.st_size), 'mtime':str(stat.st_mtime)})
                                    for name,stat in gridftp.ls(origin + path, DT_REG)
                                        if name not in exclude)
#    for url,md5 in pool.imap_unordered(gridftp.md5, urls):
#        name = basename(url)
#        meta[name]['md5'] = str(md5)
    return meta

def get_s3_meta(bucket):
    objects = [bucket.Object(so.key) for so in bucket.objects.all()]
    meta = dict((o.key, o.metadata) for o in objects)
    return meta

def diff_gridftp(bucket, origin, path, pool_size, dry_run):
    gridftp_meta = get_gridftp_meta(origin, path, pool_size)
    s3_meta = get_s3_meta(bucket)

    gridftp_names = set(gridftp_meta.keys())
    s3_names = set(s3_meta.keys())
    print('In s3, not in gridftp', [(n, s3_meta[n]) for n in s3_names - gridftp_names])
    print('In gridftp, not in s3', [(n, gridftp_meta[n]) for n in gridftp_names - s3_names])
    common_names = s3_names.intersection(gridftp_names)
    mismatched_meta = []
    for name in common_names:
        try:
            s3_meta[name].pop('md5')
        except KeyError:
            pass
        if gridftp_meta[name] != s3_meta[name]:
            mismatched_meta.append((name, s3_meta[name], gridftp_meta[name]))
    print('Metadata mismatch:', mismatched_meta)

def set_gridftp(bucket, origin, path, pool_size, dry_run):
    from cta_relay import gridftp
    from cta_relay.gridftp import DT_REG
    uploaded_files = set(o.key for o in bucket.objects.all())

    meta = dict((name, {'size':str(stat.st_size), 'mtime':str(stat.st_mtime)})
                                    for name,stat in gridftp.ls(origin + path, DT_REG))
    urls = [origin + os.path.join(path, name)
                                    for name in meta if name not in uploaded_files]
    print('Files uploaded:', len(uploaded_files))
    print('Files found:', len(meta))
    print('Files to process:', len(urls))
    if dry_run:
        return
    for url,md5 in pool.imap_unordered(gridftp.md5, urls):
        name = basename(url)
        meta[name]['md5'] = str(md5)
        obj = bucket.Object(name)
        obj.put(Metadata=meta[name])
        print(name, meta[name])

def show(bucket, obj=None):
    if obj:
        o = bucket.Object(obj)
        print(o.key, o.content_length, o.metadata)
    else:
        for so in bucket.objects.all():
            o = bucket.Object(so.key)
            print(o.key, o.content_length, o.metadata)
