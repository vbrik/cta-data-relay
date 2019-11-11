import sys
from pprint import pprint
from multiprocessing.pool import ThreadPool

def set(bucket, origin, path, pool_size, dry_run):
    from cta_relay import gridftp
    from cta_relay.gridftp import DT_REG
    pool = ThreadPool(pool_size)
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
