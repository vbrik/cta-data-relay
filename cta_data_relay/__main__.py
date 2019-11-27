#!/usr/bin/env python3
import argparse
import sys
import boto3
from boto3.s3.transfer import TransferConfig
import os
from os.path import basename
from subprocess import run, PIPE, CalledProcessError
import time
import threading
from itertools import repeat
import signal

def s3_to_gridftp(bucket, gridftp_url, gridftp_path, tempdir, obj, dry_run):
    from cta_data_relay import s3zstd
    from cta_data_relay import gridftp
    if obj is None:
        print('Retrieving list of objects')
        # Find objects that haven't been transited. Since we use compression even on
        # empty files, only objects whose bodies have been "emptied" can have size==0
        objects = [bucket.Object(o.key) for o in bucket.objects.all() if o.size > 0]
    else:
        objects = [bucket.Object(obj)]
    print('Un-downloaded keys:', [o.key for o in objects])

    for obj in objects:
        print('Downloading', obj.key)
        s3zstd.zdownload(obj, tempdir, dry_run)
        src_url = 'file://' + os.path.join(tempdir, obj.key)
        dst_url = gridftp_url + os.path.join(gridftp_path, obj.key)
        print('Uploading', dst_url)
        try:
            gridftp.copy(src_url, dst_url)
        except gridftp.gfal2.GError as e:
            if e.code == 17:
                print('Exception:', e)
            else:
                raise
        os.remove(os.path.join(tempdir, obj.key))
        if dry_run:
            continue
        obj.put(Metadata=obj.metadata) # empty object body

def main():
    parser = argparse.ArgumentParser(
            prog='cta-data-relay',
            description='',
            formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(
                                            prog, max_help_position=25, width=90))
    actions_grp = parser.add_argument_group(title='Actions',
            description='(exactly one must be specified)')
    actions_mxgrp = actions_grp.add_mutually_exclusive_group(required=True)
    actions_mxgrp.add_argument('--local-to-s3', action='store_true',
            help='Upload local files to S3 storage')
    actions_mxgrp.add_argument('--s3-to-gridftp', action='store_true',
            help='Move files from S3 to gridftp storage')
    actions_mxgrp.add_argument('--meta-show', action='store_true',
            help='Show S3 metadata')
    actions_mxgrp.add_argument('--meta-vs-gridftp', action='store_true',
            help='Compare S3 metadata vs gridftp storage')
    actions_mxgrp.add_argument('--meta-vs-local', action='store_true',
            help='Compare S3 metadata vs local storage')
    actions_mxgrp.add_argument('--meta-set-gridftp', action='store_true',
            help='Set S3 metadata to match gridftp storage')

    misc_grp = parser.add_argument_group('Miscellaneous options')
    misc_grp.add_argument('--local-path', metavar='PATH',
            help='local source file or directory')
    misc_grp.add_argument('--timeout', metavar='SECONDS', type=int,
            help='terminate after this amount of time')
    misc_grp.add_argument('--tempdir', metavar='PATH', default='/tmp',
            help='directory for (de)compression')
    misc_grp.add_argument('--dry-run', default=False, action='store_true',
            help='dry run')

    s3_grp = parser.add_argument_group('S3 options')
    s3_grp.add_argument('--s3-url', metavar='URL', default='https://rgw.icecube.wisc.edu',
            help='S3 endpoint URL')
    s3_grp.add_argument('-b', '--bucket', required=True,
            help='S3 bucket name')
    s3_grp.add_argument('-i', dest='access_key_id',
            help='S3 access key id')
    s3_grp.add_argument('-k', dest='secret_access_key',
            help='S3 secret access key')
    s3_grp.add_argument('--s3-threads', metavar='NUM', type=int, default=80,
            help='maximum number of S3 transfer threads')
    s3_grp.add_argument('--object', metavar='KEY',
            help='operate on specific S3 object only')

    grid_grp = parser.add_argument_group('GridFTP options')
    grid_grp.add_argument('--gridftp-url', metavar='URL', default='gsiftp://gridftp.icecube.wisc.edu',
            help='GridFTP endpoint URL')
    grid_grp.add_argument('--gridftp-path', metavar='PATH',
            help='GridFTP path')
    grid_grp.add_argument('--gridftp-threads', metavar='NUM', type=int, default=45,
            help='gridftp worker pool size')

    args = parser.parse_args()
    if args.timeout:
        signal.alarm(args.timeout)
    if not os.path.isdir(args.tempdir):
        parser.exit(f'Invalid argument: {args.tempdir} is not a directory')

    s3 = boto3.resource('s3', 'us-east-1', endpoint_url=args.s3_url,
            aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)
    bucket = s3.Bucket(args.bucket)
    bucket.create()

    compr_threads = max(1, int(os.cpu_count()/2))
    multipart_size = 2**20

    if args.local_to_s3:
        import cta_data_relay.s3zstd
        tx_config = TransferConfig(max_concurrency=args.s3_threads,
                                        multipart_threshold=multipart_size, 
                                        multipart_chunksize=multipart_size)
        if args.local_path is None:
            parser.exit(f'Missing required argument --local-path')
        if os.path.isfile(args.local_path):
            file_info = [(args.local_path, os.path.getsize(args.local_path))]
        else:
            file_info = [(de.path, de.stat().st_size)
                                    for de in os.scandir(args.local_path) if de.is_file()]
        cta_data_relay.s3zstd.zupload(bucket, file_info, args.tempdir, compr_threads,
                                                            tx_config, args.dry_run)
    elif args.s3_to_gridftp:
        if args.gridftp_path is None:
            parser.exit(f'Missing required argument --gridftp-path')
        s3_to_gridftp(bucket, args.gridftp_url, args.gridftp_path, args.tempdir,
                                        args.object, args.dry_run)
    elif args.meta_set_gridftp:
        import cta_data_relay.meta
        if args.gridftp_path is None:
            parser.exit(f'Missing required argument --gridftp-path')
        cta_data_relay.meta.set_gridftp(bucket, args.gridftp_url, args.gridftp_path,
                                                    args.gridftp_threads, args.dry_run)
    elif args.meta_show:
        import cta_data_relay.meta
        cta_data_relay.meta.show(bucket, args.object)
    elif args.meta_vs_gridftp:
        import cta_data_relay.meta
        if args.gridftp_path is None:
            parser.exit(f'Missing required argument --gridftp-path')
        cta_data_relay.meta.diff_gridftp(bucket, args.gridftp_url, args.gridftp_path,
                                                    args.gridftp_threads, args.dry_run)
    elif args.meta_vs_local:
        import cta_data_relay.meta
        if args.local_path is None:
            parser.exit(f'Missing required argument --local-path')
        cta_data_relay.meta.diff_local(bucket, args.local_path)
    else:
        parser.exit('Usage error. Unexpected command.')

if __name__ == '__main__':
    sys.exit(main())

