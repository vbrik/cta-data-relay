# CTA Relay
This applications relays data from local disk to a GridFTP endpoint via an S3 bucket. CTA Relay is designed for an environment with very specific networking restrictions.

"CTA" in CTA Relay refers to the Cherenkov Telescope Array Observatory, the research project for which this application was written to assist with certain data transfers.

# Overview
CTA Relay is like a highly-specialized rsync.

It operates roughly as follows:

On the source host:
1. Build a list of files in a directory
1. Retrieve a list of files that have been transferred previously from metadata stored in an S3 bucket
1. Compress and upload to the S3 bucket files that haven't been uploaded previously

On the relay host:
1. Download unprocessed files from the bucket, decompress them, and upload them to the GridFTP endpoint
1. Update metadata in S3 bucket to indicate the file has been processed

CTA Relay can also perform various metadata operations, such setting the S3 bucket metadata to reflect what files are already in the GridFTP location (so that they are not re-uploaded).


# Installation
CTA Relay requires Python3 and [zstd](https://facebook.github.io/zstd/) (e.g. `yum install -y zstd`).

    git clone https://github.com/vbrik/cta-relay.git
    cd cta-relay
    python3 -m venv .venv
    pip install -r requirements.txt

Sub-commands that need to connect to a GridFTP server require the [gfal2](https://dmc.web.cern.ch/projects/gfal-2/home) library and its GridFTP plug-in. While not strictly necessary, additional packages are usually needed to use GridFTP in practice. See [Dockerfile](Dockerfile) for GridFTP-related dependencies.

# AWS Authentication
If AWS credentials are not supplied as command-line arguments, CTA Relay will rely on [boto3](https://boto3.readthedocs.io) to determine them. At the time of writing, this meant environmental variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or file `~/.aws/config`, which could look like this:
```
[default]
aws_access_key_id = XXX
aws_secret_access_key = YYY
```

# Usage
CTA Relay has three modes of operation.

"Local-to-S3" mode copies data from local files to an S3 bucket.

"S3-to-GridFTP" mode moves data from the S3 bucket to an GridFTP endpoint.

"Metadata" mode allows examining and manipulation of metadata that is stored in S3.

Run the application with the `--help` flag for more information.
