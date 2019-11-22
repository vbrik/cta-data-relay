- tempdir is not cleaned-up
- weird performance inssues in kubernetes
    - getting i/o blocked for long time (ceph?)
        - decompress takes a long time
        - stutter during gridftp transfers
    - poor gridftp upload performance
        - only 50MB/s up (/data/wipac limit?)
        - stutter (see above)
        - params.nbstreams not respected?
- s3 progress
    - time column changes width
    - update_rate is sometimes negative
- md5 is pretty slow
- if gfal2.copy fails in the middle (e.g. error, timeout),
    partial file is not cleaned-up. On the next run gfal will
    not overwrite the file, but will empty its content in s3,
    which indicates file does not need to be transitted
- Docker file installs git to make k8s container build faster
    There has to be a better way to decouple code and deployment
    - no nice way to test code in k8s though wo creating a
        new container/deployment
- if a file is created on the gridftp side and then on the source side
    it will not be overwritten
