#!/usr/bin/python

from __future__ import print_function
import sys
import subprocess
import os
import re
import argparse
import gzip
import calendar
import radix
from datetime import datetime
from bz2 import BZ2File
from time import sleep
from netaddr import IPSet
from multiprocessing import Pool

import mrtx

verbose = False
warning = False
threads = False
logging = False

re_file_rv = re.compile('rib.(\d+).(\d\d\d\d).bz2')
re_file_rr = re.compile('bview.(\d+).(\d\d\d\d).gz')

reserved_ipv4 = IPSet (['0.0.0.0/8',                                        # host on this network (RFC1122)
                        '10.0.0.0/8','172.16.0.0/12','192.168.0.0/16',      # private address space (RFC1918)
                        '100.64.0.0/10',                                    # shared address space (RFC6598)
                        '127.0.0.0/8',                                      # loopback (RFC1122)
                        '169.254.0.0/16',                                   # linklocal (RFC3927)
                        '192.0.0.0/24',                                     # special purpose (RFC6890)
                        '192.0.0.0/29',                                     # DS-lite (RFC6333)
                        '192.0.2.0/24','198.51.100.0/24','203.0.113.0/24',  # test net 1-3 (RFC5737)
                        '224.0.0.0/4',                                      # multicast address space
                        '240.0.0.0/4',                                      # future use (RFC1122)
                        '255.255.255.255/32'                                # limited broadcast
                    ]) 

'''
OUTPUT FORMAT:

timestamp|date ; input type (RIB|UPDATE) ; source (route-views.xyz| rrcXY) ; \ 
    #ipv4-prefixes/pfxlength (1..32) ; #ipv4 moas ; #ipv4 bogus \ 
    [; #ipv6-prefix/pfxlength ; #ipv6 moas ; #ipv6 bogus ]

NOTE:

 - #ips covered can be derived from #pfx/pfx_len
'''

def print_log(*objs):
    if logging or verbose:
        print("[LOGS] .", *objs, file=sys.stdout)

def print_info(*objs):
    if verbose:
        print("[INFO] ..", *objs, file=sys.stdout)

def print_warn(*objs):
    if warning or verbose:
        print("[WARN] ", *objs, file=sys.stderr)

def print_error(*objs):
    print("[ERROR] ", *objs, file=sys.stderr)

def getPtree (fin):
    f = (BZ2File(fin, 'rb'), gzip.open(fin, 'rb'))[fin.lower().endswith('.gz')]
    data = mrtx.parse_mrt_file(f, print_progress=verbose)
    f.close()
    ptree = radix.Radix()
    for prefix, origins in data.items():
        pnode = ptree.add(prefix)
        pnode.data['asn'] = origins
    return ptree

def getStats (ptree):
    print_log("call getStats")
    pfxlen = dict()
    for p in ptree:
        pl = int(p.prefixlen)
        if pl not in pfxlen:
            pfxlen[pl] = list()
        pfxlen[pl].append(p.prefix)
    pl_dict = dict()
    pfxmoas = 0
    for pl in pfxlen:
        pl_dict[pl] = len(pfxlen[pl])
        if pl_dict[pl] > 1:
            pfxmoas += 1 
    pkeys = sorted(pfxlen.keys(),reverse=False)
    prefixIPs = IPSet()
    for pk in pkeys:
        print_info ("prefix length: "+str(pk)+", #prefixes: "+ str(len(pfxlen[pk])))
        prefixIPs = prefixIPs | IPSet(pfxlen[pk])
    num_bogus_ips = len(prefixIPs & reserved_ipv4)
    return pl_dict, num_bogus_ips, pfxmoas

def getDiffs (pt0, pt1):
    pass

def outputStats (pl, pb, pm):
    print_log("call outputStats")
    output = ''
    for p in sorted(pl.keys()):
        output += str(pl[p])+'; '
    output += str(pb)+'; '
    output += str(pm)
    print(output)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    parser.add_argument('-t', '--threads',      help='Use threads, for parallel and faster processing.', action='store_true')
    parser.add_argument('-n', '--numworker',    help='Number of worker threads (Default 4).', default='4')
    parser.add_argument('-i', '--inputdir',     help='Path to bgpdump files, bulk mode only. Defaults to current directory.', default='.')
    parser.add_argument('-o', '--outputdir',    help='Path to store output, bulk mode only. Defaults to INPUTDIR directory.', default='')
    parser.add_argument('-s', '--single',       help='Convert a single file, output to <filename>.gz in current directory.', default='')
    parser.add_argument('-b', '--bulk',         help='Convert a bunch of files, see also input/outputdir.', action='store_true')
    args = vars(parser.parse_args())
    
    global verbose
    verbose   = args['verbose']

    global warning
    warning   = args['warning']

    global logging
    logging = args['logging']

    threads = args['threads']

    bulk = args['bulk']
    single = args['single']

    num_worker = 4
    try:
        num_worker = int(args['numworker'])
    except:
        print_warn("Unable to set number of worker threads to %s" % (args['numworker']))

    print_log("START: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    if bulk:
        print_log('mode: bulk')
        ipath = args['inputdir']
        opath = args['outputdir']
        if len(opath) == 0:
            opath = ipath
        if not (os.path.isdir(ipath) and os.path.isdir(opath)):
            print_error("Invalid path for INPUTDIR and/or OUTPUTDIR!")
            exit(1)
        
        maptype, subtype = parsePathname(ipath)

        if not (os.path.isdir(ipath) and os.path.isdir(opath)):
            print_error("Invalid path for INPUTDIR and/or OUTPUTDIR!")
            exit(1)
        print_info("INPUTDIR (%s), OUTPUTDIR (%s)." % (ipath,opath))
        all_files = [ f for f in os.listdir(ipath) if os.path.isfile(os.path.join(ipath,f)) ]
        work_load = []
        for f in all_files:
            opts = parseFilename(f, ipath, opath, maptype, subtype)
            if len(opts) > 0:
                work_load.append(opts)
        if threads:
            pool = Pool(num_worker)
            pool.map(convertFile, work_load)
        else:
            for work in work_load:
                convertFile(work)

    elif single:
        print_log("mode: single")
        if os.path.isfile(single):
            pt0 = getPtree(single)
            pl, pb, pm = getStats(pt0)
            outputStats(pl,pb,pm)
        else:
            print_error("File not found (%s)!" % (single))
    else:
        print_error("Missing parameter: choose bulk or single mode!")
        exit(1)

    print_log("FINISH: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == "__main__":
    main()