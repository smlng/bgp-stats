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
from netaddr import IPSet, IPNetwork
from multiprocessing import Pool

import mrtx

verbose = False
warning = False
threads = False
logging = False

re_file_rv = re.compile('rib.(\d+).(\d\d\d\d).bz2')
re_file_rr = re.compile('bview.(\d+).(\d\d\d\d).gz')

re_path_rv = re.compile('.*/([a-z0-9\.-]+)/bgpdata/\d\d\d\d.\d\d/RIBS.*')
re_path_rr = re.compile('.*/(rrc\d\d)/\d\d\d\d.\d\d.*')

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
        pnode.data['asn'] = list()
        pnode.data['moas'] = 0
        for o in origins:
            pnode.data['asn'].append(o)
            pnode.data['moas'] += 1
        #print_info(str(pnode.data['moas'])+" : "+','.join(str(x) for x in pnode.data['asn']))
    return ptree

def getStats (ptree):
    print_log("call getStats")
    pfxlen = dict()
    pfxmoas = 0
    for p in ptree:
        pl = int(p.prefixlen)
        if p.data['moas'] > 1:
            pfxmoas += 1
        if pl not in pfxlen:
            pfxlen[pl] = list()
        pfxlen[pl].append(p.prefix)
    pl_dict = dict()
    # init with all 0
    for i in range(32):
        pl_dict[i+1] = 0

    for pl in pfxlen:
        pl_dict[pl] = len(pfxlen[pl])

    pkeys = sorted(pfxlen.keys(),reverse=False)
    prefixIPs = IPSet()
    for pk in pkeys:
        print_info ("prefix length: "+str(pk)+", #prefixes: "+ str(len(pfxlen[pk])))
        prefixIPs = prefixIPs | IPSet(pfxlen[pk])
    num_bogus_ips = len(prefixIPs & reserved_ipv4)
    num_pfx_ips = len(prefixIPs)
    return pl_dict, num_pfx_ips, num_bogus_ips, pfxmoas

def getDiffs (pt0, pt1):
    print_log("call getDiffs")
    num_ips_changed = 0
    num_pfx_agg = 0
    num_pfx_deagg = 0
    pt0IPs = IPSet(pt0.prefixes()) - reserved_ipv4
    pt1IPs = IPSet(pt1.prefixes()) - reserved_ipv4
    num_ips_new = len(pt1IPs - pt0IPs)
    num_ips_del = len(pt0IPs - pt1IPs)
    for pn0 in pt0:
        if IPNetwork(pn0.prefix) not in reserved_ipv4:
            pn1 = pt1.search_best(pn0.network)
            if pn1:
                if pn0.prefix != pn1.prefix:
                    if pn0.prefixlen > pn1.prefixlen:
                        num_pfx_agg += 1
                    elif pn0.prefixlen < pn1.prefixlen:
                        num_pfx_deagg += 1
                    num_ips_changed += 2 ** (32 - pn0.prefixlen)
    ret = [len(pt0IPs), len(pt1IPs), num_ips_new, num_ips_del, num_ips_changed, num_pfx_agg, num_pfx_deagg]
    return ret

def parseFilename(fin):
    print_log("call parseFilename (%s)" % (fin))

    maptype = 'none'
    subtype = 'none'
    pn, fn = os.path.split(fin)

    if re_path_rr.match(pn):
        m = re_path_rr.match(pn)
        maptype = 'riperis'
        subtype = m.group(1)
    elif re_path_rv.match(pn):
        m = re_path_rv.match(pn)
        maptype = 'routeviews'
        subtype = m.group(1)
    else:
        print_warn("Unknown BGP data source (pathname).")

    date = '19700101'
    time = '0000'
    if re_file_rr.match(fn):
        maptype = 'riperis'
        m = re_file_rr.match(fn)
        date = m.group(1)
        time = m.group(2)
    elif re_file_rv.match(fn):
        maptype = 'routeviews'
        m = re_file_rv.match(fn)
        date = m.group(1)
        time = m.group(2)
    else:
        print_warn("Unknown BGP data source (filename).")
    dt = "%s-%s-%s %s:%s" % (str(date[0:4]),str(date[4:6]),str(date[6:8]),str(time[0:2]),str(time[2:4]))
    ts = int((datetime.strptime(dt, "%Y-%m-%d %H:%M") - datetime(1970, 1, 1)).total_seconds())
    return ts, maptype, subtype

def outputStats (ts, mt, st, pl, pi, pb, pm):
    output = 'STATS;'+str(ts)+';'+mt+';'+st+';'
    for p in sorted(pl.keys()):
        output += str(pl[p])+';'
    output += str(pi)+';'
    output += str(pb)+';'
    output += str(pm)
    print(output)

def outputDiffs(ts0,ts1,mt,st,diffs):
    output = 'DIFFS;'+str(ts0)+';'+str(ts1)+';'+mt+';'+st+';'
    output += ';'.join(str(x) for x in diffs)
    print(output)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    parser.add_argument('-t', '--threads',      help='Use N threads, for parallel and faster processing.', type=int, default=1)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--single',       help='Convert a single file, output to <filename>.gz in current directory.')
    group.add_argument('-b', '--bulk',         help='Convert a bunch of files, see also input/outputdir.')
    parser.add_argument('-r', '--recursive',   help='Search directories recursivly if in bulk mode.', action='store_true')
    args = vars(parser.parse_args())
    
    global verbose
    verbose   = args['verbose']

    global warning
    warning   = args['warning']

    global logging
    logging   = args['logging']

    recursive = args['recursive']
    threads   = args['threads']

    bulk      = args['bulk']
    single    = args['single']

    print_log("START: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    if bulk:
        print_log('mode: bulk')

        if not (os.path.isdir(bulk)):
            print_error("Invalid path for bulk processing!")
            exit(1)

        all_files = []
        if recursive:
            for dirpath, dirnames, filenames in os.walk(bulk):
                for filename in [f for f in filenames if (re_file_rv.match(f) or re_file_rr.match(f))]:
                    all_files.append(os.path.join(dirpath, filename))
        else:
            for filename in [f for f in os.listdir(bulk) if (re_file_rv.match(f) or re_file_rr.match(f))]:
                all_files.append(os.path.join(bulk, filename))

        for i in range(len(all_files)-1):
            fin0 = all_files[i]
            ts0, mt0, st0 = parseFilename(fin0)

            fin1 = all_files[i+1]
            ts1, mt1, st1 = parseFilename(fin1)

            if (mt0 == mt1) and (st0 == st1):
                pt0 = getPtree(fin0)
                pl0, pi0, pb0, pm0 = getStats(pt0)

                pt1 = getPtree(fin1)
                pl1, pi1, pb1, pm1 = getStats(pt1)
                diffs = getDiffs(pt0, pt1)

                outputStats(ts0,mt0,st0,pl0,pi0,pb0,pm0)
                outputStats(ts1,mt1,st1,pl1,pi1,pb1,pm1)
                outputDiffs(ts0,ts1,mt0,st0,diffs)
            
    elif single:
        print_log("mode: single")
        if os.path.isfile(single):
            ts0, mt0, st0 = parseFilename(os.path.abspath(single))
            pt0 = getPtree(single)
            pl0, pi0, pb0, pm0 = getStats(pt0)
            outputStats(ts0,mt0,st0,pl0,pi0,pb0,pm0)
        else:
            print_error("File not found (%s)!" % (single))
    else:
        print_error("Missing parameter: choose bulk or single mode!")
        exit(1)

    print_log("FINISH: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == "__main__":
    main()