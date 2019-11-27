- when metadata is compared, only sizes are checked
- tempdir is not cleaned-up
- hashlib's md5 is pretty slow
- if gfal2.copy fails in the middle (e.g. error, timeout),
    partial file is not cleaned-up. On the next run gfal will
    not overwrite the file, but will empty its content in s3,
    which indicates file does not need to be transitted
- if a file is created on the gridftp side and then on the source side
    it will not be overwritten
