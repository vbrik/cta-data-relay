# cta
CTA

# s3cmd
$ g rgw ~/.s3cfg
host_base = rgw.icecube.wisc.edu
host_bucket = rgw.icecube.wisc.edu

# commands
$ s3cmd ls --recursive s3://cta
$ s3cmd ls s3://cta// # to see /scratch.py
radosgw-admin bucket list --bucket=cta #to see objects
s3cmd rb --force --recursive s3://cta
