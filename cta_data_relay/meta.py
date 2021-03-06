from multiprocessing.pool import ThreadPool
import os
from os.path import basename
from pprint import pprint
import random
import sys

def _get_gridftp_meta_sizes(origin, path, pool_size):
    from cta_data_relay import gridftp
    from cta_data_relay.gridftp import DT_REG
    meta = dict((name, {'size':str(stat.st_size)})
                                    for name,stat in gridftp.ls(origin + path, DT_REG))
#    pool = ThreadPool(pool_size)
#    for url,md5 in pool.imap_unordered(gridftp.md5, urls):
#        name = basename(url)
#        meta[name]['md5'] = str(md5)
    return meta

def _get_s3_meta_sizes(bucket):
    objects = [bucket.Object(so.key) for so in bucket.objects.all()]
    meta = dict((o.key, {'size':o.metadata['size']}) for o in objects)
    return meta

# XXX Currently, only sizes are compared
def diff_gridftp(bucket, origin, path, pool_size, dry_run):
    gridftp_meta_sizes = _get_gridftp_meta_sizes(origin, path, pool_size)
    s3_meta_sizes = _get_s3_meta_sizes(bucket)

    gridftp_names = set(gridftp_meta_sizes.keys())
    s3_names = set(s3_meta_sizes.keys())
    print('Name in s3, not in gridftp', [(n, s3_meta_sizes[n]) for n in s3_names - gridftp_names])
    print('Name in gridftp, not in s3', [(n, gridftp_meta_sizes[n]) for n in gridftp_names - s3_names])
    common_names = s3_names.intersection(gridftp_names)
    mismatched_meta = []
    for name in common_names:
        if gridftp_meta_sizes[name] != s3_meta_sizes[name]:
            mismatched_meta.append((name, s3_meta_sizes[name], gridftp_meta_sizes[name]))
    print('Metadata mismatch:', mismatched_meta)

def diff_local(bucket, local_path):
    local_meta_sizes = dict((basename(de.path), {'size':str(de.stat().st_size)})
                                    for de in os.scandir(local_path) if de.is_file())
    s3_meta_sizes = _get_s3_meta_sizes(bucket)

    local_names = set(local_meta_sizes.keys())
    s3_names = set(s3_meta_sizes.keys())
    print('Name in s3, not in local', [(n, s3_meta_sizes[n]) for n in s3_names - local_names])
    print('Name in local, not in s3', [(n, local_meta_sizes[n]) for n in local_names - s3_names])
    common_names = s3_names.intersection(local_names)
    mismatched_meta = []
    for name in common_names:
        if local_meta_sizes[name] != s3_meta_sizes[name]:
            mismatched_meta.append((name, s3_meta_sizes[name], local_meta_sizes[name]))
    print('Metadata mismatch:', mismatched_meta)

def prune_not_in_gridftp(bucket, origin, path, dry_run):
    from cta_data_relay import gridftp
    from cta_data_relay.gridftp import DT_REG
    gridftp_files = [name for name,stat in gridftp.ls(origin + path, DT_REG)]
    deleted_files = [o.key for o in bucket.objects.all()
                                    if o.size == 0 and o.key not in gridftp_files]
    for key in deleted_files:
        show(bucket, obj=key)
    if dry_run:
        return
    bucket.delete_objects(Delete={'Objects':[{'Key':name} for name in deleted_files]})

def set_gridftp(bucket, origin, path, pool_size, dry_run):
    from cta_data_relay import gridftp
    from cta_data_relay.gridftp import DT_REG
    uploaded_files = set(o.key for o in bucket.objects.all())

    meta = dict((name, {'size':str(stat.st_size)})
                                    for name,stat in gridftp.ls(origin + path, DT_REG))
    urls = [origin + os.path.join(path, name)
                                    for name in meta if name not in uploaded_files]
    # shuffle URLs to spread the load across Lustre OSSes when striping is disabled
    random.shuffle(urls)
    print('Files uploaded:', len(uploaded_files))
    print('Files found:', len(meta))
    print('Files to process:', len(urls))
    if dry_run:
        return
    pool = ThreadPool(pool_size)
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
