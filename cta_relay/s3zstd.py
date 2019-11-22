import sys
from pprint import pprint
import boto3
from boto3.s3.transfer import TransferConfig
import os
from os.path import basename
from subprocess import run, PIPE
import time
import threading
from functools import partial
import hashlib
from functools import partial

class ProgressMeter(object):
    # To simplify, assume this is hooked up to a single operation
    def __init__(self, label, size, update_interval=10):
        self._label = label
        self._size = size
        self._count = 0
        self._first_time = None
        self._first_count = None
        self._update_interval = update_interval
        self._last_update_time = None
        self._last_update_count = None
        self._lock = threading.Lock()

    def __readable_size(self, size):
        if size < 10**3:
            return '%s B' % int(size)
        elif size < 10**6:
            return '%.2f KiB' % (size/10**3)
        elif size < 10**9:
            return '%.2f MiB' % (size/10**6)
        elif size < 10**12:
            return '%.2f GiB' % (size/10**9)
        else:
            return '%.2f TiB' % (size/10**12)

    def __readable_time(self, time):
        time = int(round(time))
        seconds = time % 60
        minutes = (time // 60) % 60
        hours = time // (60 * 60)
        if hours:
            return f'{hours}h {minutes}m {seconds}s'
        elif minutes:
            return f'{minutes}m {seconds}s'
        else:
            return f'{seconds}s'

    def __call__(self, num_bytes):
        with self._lock:
            now = time.time()
            if self._first_time is None:
                self._first_time = now
                self._first_count = num_bytes
                self._last_update_time = now
                self._last_update_count = num_bytes
            self._count += num_bytes
            t_observed = now - self._first_time
            t_since_update = now - self._last_update_time
            b_since_update = self._count - self._last_update_count

            rs = partial(self.__readable_size)
            rt = partial(self.__readable_time)
            if self._count == self._size:
                sys.stdout.write(f'{self._label} {rs(self._size)} in ~{rt(t_observed)}\n')
                sys.stdout.flush()
                return
            if t_since_update >= self._update_interval:
                percent = (self._count / self._size) * 100
                update_delta = self._count - self._last_update_count 
                update_rate = update_delta / t_since_update # XXX why is this negative sometimes?
                average_rate = self._count / t_observed
                t_remaining = (self._size - self._count) / average_rate
                sys.stdout.write(
                        f'{self._label: <20} {rt(t_observed)}  '
                        f'{rs(self._count): >10} / {rs(self._size)} {percent: 3.0f}%  '
                        f'{rs(update_rate)}/s {rs(average_rate)}/s  '
                        f'ETA: {rt(t_remaining)}\n')
                sys.stdout.flush()
                self._last_update_time = now
                self._last_update_count = self._count

def _md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 2**20), b''):
            d.update(buf)
    return d.hexdigest()

def zdownload(obj, tempdir, dry_run):
    zstd_path = os.path.join(tempdir, obj.key + '.zst')
    obj.download_file(zstd_path, Callback=ProgressMeter(zstd_path, obj.content_length))
    run(['nice', '-n', '19', 'zstd', '--force', '--decompress', '--rm', zstd_path],
                                            check=True, stdout=sys.stdout, stderr=PIPE)

def zupload(bucket, file_info, tempdir, threads, tx_config, dry_run=False):
    uploaded_files = set(o.key for o in bucket.objects.all())
    unuploaded_files = [(p,s) for p,s in file_info if basename(p) not in uploaded_files]
    print('Uploaded:', len(uploaded_files))
    print('Un-uploaded:', len(unuploaded_files),
            sum(size for name,size in unuploaded_files)/10**9, 'GB')
    if dry_run:
        return
    for path,size in unuploaded_files:
        zstd_path = os.path.join(tempdir, basename(path) + '.zst')
        run(['nice', '-n', '19', 'zstd', '--force', '--threads=%s' % threads, path, '-o', zstd_path],
                                            check=True, stdout=PIPE, stderr=PIPE)
        md5 = _md5sum(path)
        bucket.upload_file(zstd_path, basename(path), Config=tx_config,
                Callback=ProgressMeter(zstd_path, os.path.getsize(zstd_path)),
                ExtraArgs={'Metadata': {'size': str(size), 'md5':md5}})
        os.remove(zstd_path)
