#!/usr/bin/env python3
import argparse
import sys
from pprint import pprint
import boto3
from boto3.s3.transfer import TransferConfig
import os
from os.path import basename
from subprocess import run, PIPE, CalledProcessError
import time
import threading
from itertools import repeat
import signal


def main():
    parser = argparse.ArgumentParser(
            prog='cta-relay',
            description='',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    actions_grp = parser.add_argument_group(title='Actions', description='(exactly one action must be specified)')
    actions_mxgrp = actions_grp.add_mutually_exclusive_group(required=True)
    actions_mxgrp.add_argument('--local-to-s3', action='store_true',
            help='Upload local files to S3 storage')
    actions_mxgrp.add_argument('--s3-to-gridftp', action='store_true',
            help='Move files from S3 to gridftp storage')
    actions_mxgrp.add_argument('--gridftp-to-meta', action='store_true',
            help='Set S3 metadata to match gridftp storage')
    actions_mxgrp.add_argument('--show-meta', action='store_true',
            help='Show S3 metadata')

    parser.add_argument('path', metavar='PATH', help='local source dir')

    misc_grp = parser.add_argument_group('Miscellaneous options')
    misc_grp.add_argument('-n', '--dry-run', default=False, action='store_true',
            help='dry run')
    misc_grp.add_argument('--timeout', metavar='SECONDS', type=int,
            help='terminate after this amount of time')
    misc_grp.add_argument('--tempdir', metavar='PATH', default='/tmp',
            help='directory for (de)compression')
    misc_grp.add_argument('--compr-threads', metavar='N', type=int,
            help='number of threads to use for (de)compression, instead of half of available cores')

    s3_grp = parser.add_argument_group('S3 options')
    s3_grp.add_argument('--s3-url', metavar='URL', default='https://rgw.icecube.wisc.edu',
            help='s3 endpoint URL')
    s3_grp.add_argument('--bucket', default='cta-dev',
            help='S3 bucket name')
    s3_grp.add_argument('-i', dest='access_key_id',
            help='S3 access key id')
    s3_grp.add_argument('-k', dest='secret_access_key',
            help='S3 secret access key')
    s3_grp.add_argument('--s3-threads', metavar='N', type=int, default=80,
            help='maximum number of S3 transfer threads')
    s3_grp.add_argument('--multipart-size', metavar='B', type=int, default=2**20,
            help='multipart threshold and chunk size')
    s3_grp.add_argument('--object', metavar='NAME',
            help='operate on specific S3 object only')

    grid_grp = parser.add_argument_group('GridFTP options')
    grid_grp.add_argument('--gridftp-url', metavar='URL', default='gsiftp://gridftp.icecube.wisc.edu',
            help='GridFTP endpoint URL')
    grid_grp.add_argument('--gridftp-path', metavar='PATH', default='/data/wipac/CTA/cta-sync-test',
            help='GridFTP path')
    grid_grp.add_argument('--gridftp-threads', metavar='N', type=int, default=45,
            help='gridftp worker pool size')

    args = parser.parse_args()
    if args.timeout:
        signal.alarm(args.timeout)
    if not os.path.isdir(args.tempdir):
        parser.exit(f'Invalid argument: {args.tempdir} is not a directory')
    if args.compr_threads is None:
        args.compr_threads = max(1, int(os.cpu_count()/2))
    pprint(args)

    s3 = boto3.resource('s3', 'us-east-1', endpoint_url=args.s3_url,
            aws_access_key_id=args.access_key_id, aws_secret_access_key=args.secret_access_key)
    bucket = s3.Bucket(args.bucket)
    bucket.create()

    if args.local_to_s3:
        import cta_relay.s3
        tx_config = TransferConfig(max_concurrency=args.s3_threads,
                                        multipart_threshold=args.multipart_size, 
                                        multipart_chunksize=args.multipart_size)
        cta_relay.s3.upload(bucket, args.path, args.tempdir, args.compr_threads, tx_config, args.dry_run)
    elif args.s3_to_gridftp:
        import cta_relay.transit
        cta_relay.transit.transit(bucket, args.gridftp_url, args.gridftp_path, args.tempdir,
                                            args.compr_threads, args.object, args.dry_run)
    elif args.gridftp_to_meta:
        import cta_relay.meta
        cta_relay.meta.bulk_set_metadata(bucket, args.gridftp_url, args.gridftp_path,
                                                    args.gridftp_threads, args.dry_run)
    elif args.show_meta:
        import cta_relay.meta
        cta_relay.meta.show_metadata(bucket, args.object)
    else:
        parser.exit('Usage error. Unexpect sub-command.')

if __name__ == '__main__':
    sys.exit(main())

