#!/usr/bin/env python
import sys
from pprint import pprint

import gfal2

gfal2.set_verbose(gfal2.verbose_level.warning)

# from dirent.h
DT_DIR = 4 # gfal2.Gfal2Context.Dirent d_type directory
DT_REG = 8 # gfal2.Gfal2Context.Dirent d_type file

def gfal2_list_dir(url, dt_filter=None):
    ctx = gfal2.creat_context()
    dirp = ctx.opendir(url)
    ret = []
    while True:
        dirent,stat = dirp.readpp()
        if dirent is None:
            break
        if dirent.d_name in ('.', '..'):
            continue
        if dt_filter is None or dirent.d_type == dt_filter:
            ret.append((dirent.d_name,stat))
    return ret

def gfal2_md5(url):
    ctx = gfal2.creat_context()
    md5 = ctx.checksum(url, 'MD5')
    return (url, md5)

def event_callback(event):
    #print event
    print("[%s] %s %s %s" % (event.timestamp, event.domain, event.stage, event.description))

def monitor_callback(src, dst, average, instant, transferred, elapsed):
    print("[%4d] %.2fMB (%.2fKB/s)\r" % (elapsed, transferred / 1048576, average / 1024)),
    sys.stdout.flush()

def copy(src_url, dst_url):
    ctx = gfal2.creat_context()
    params = ctx.transfer_parameters()
    params = ctx.transfer_parameters()
    #params.event_callback = event_callback
    params.monitor_callback = monitor_callback
    params.overwrite = False
    params.checksum_check = False
    params.timeout = 300
    r = ctx.filecopy(params, src_url, dst_url)
    #print(r)
