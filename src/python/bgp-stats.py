#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import radix
import re
import sys

from bz2 import BZ2File
from collections import OrderedDict
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from netaddr import IPSet, IPNetwork

# own imports
import mrtx

verbose = False
warning = False
logging = False

queue_limit = 7

ptree_limit = 3
ptree_cache = OrderedDict()

stats_print = []

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
    print_log("call getPtree (%s)" % (fin))
    ts, mt, st = parseFilename(fin)
    global ptree_cache
    k = str(ts)+'_'+mt+'_'+st
    if k not in ptree_cache:
        ptree_cache[k] = loadPtree(fin)
    if len(ptree_cache) > ptree_limit:
        ptree_cache.popitem(last=False)
    ptree = ptree_cache[k]
    return ptree

def loadPtree(fin):
    print_log("call loadPtree (%s)"  % (fin))
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
    num_ips_agg = 0
    num_ips_deagg = 0
    pt0IPs = IPSet(pt0.prefixes()) - reserved_ipv4
    pt1IPs = IPSet(pt1.prefixes()) - reserved_ipv4
    num_ips_new = len(pt1IPs - pt0IPs)
    num_ips_del = len(pt0IPs - pt1IPs)
    for pn0 in pt0:
        ipn0 = IPNetwork(pn0.prefix)
        if ipn0 not in reserved_ipv4:
            pn1 = pt1.search_best(str(ipn0[abs(len(ipn0)/2)]))
            if pn1:
                ipn1 = IPNetwork(pn1.prefix)
                if ipn0 != ipn1:
                    if ipn0.prefixlen > ipn1.prefixlen:
                        num_ips_agg += 2 ** (32 - ipn0.prefixlen)
                    elif ipn0.prefixlen < ipn1.prefixlen:
                        num_ips_deagg += 2 ** (32 - ipn0.prefixlen)
                    num_ips_changed += 2 ** (32 - ipn0.prefixlen)
    ret = [len(pt0IPs), len(pt1IPs), num_ips_new, num_ips_del, num_ips_changed, num_ips_agg, num_ips_deagg]
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

def singleWorker(wd, opts):
    if len(opts) != 2:
        print_error("worker: Invalid number of arguments!")
        return

    fin0 = opts[0]
    fin1 = opts[1]

    print_log("call singleWorker(\n\tfin0: %s\n\tfin1: %s)" % (fin0,fin1))

    ts0, mt0, st0 = parseFilename(fin0)
    ts1, mt1, st1 = parseFilename(fin1)
    if (mt0 == mt1) and (st0 == st1):
        pt0 = getPtree(fin0)
        pt1 = getPtree(fin1)

        pl0, pi0, pb0, pm0 = getStats(pt0)
        pl1, pi1, pb1, pm1 = getStats(pt1)
        diffs = getDiffs(pt0, pt1)

        outputStats(wd,ts0,mt0,st0,pl0,pi0,pb0,pm0)
        outputStats(wd,ts1,mt1,st1,pl1,pi1,pb1,pm1)
        outputDiffs(wd,ts0,ts1,mt0,st0,diffs)

def inputThread(file_list, stats_queue, diffs_queue):
    print_log("call inputThread")
    try:
        assert len(file_list) > 0
        for i in range(len(file_list)):
            fin = file_list[i]
            ts, mt, st = parseFilename(fin)
            pt = loadPtree(fin)
            data = [ts,mt,st,pt]
            stats_queue.put(data)
            diffs_queue.put(data)
    except Exception, e:
        print_error("inputThread: some error: %s" % e)
    finally:
        # send done to other threads to stop them
        stats_queue.put('DONE')
        diffs_queue.put('DONE')

def statsThread(queue, fout):
    print_log("start statsThread")
    errors = 0
    calls = 0
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        try:
            ts = data[0]
            mt = data[1]
            st = data[2]
            pt = data[3]
            calls += 1
            print_info("statsThread, call %d" % calls)
            pl, pi, pb, pm = getStats(pt)
            outputStats(fout, ts, mt, st, pl, pi, pb, pm)
        except:
            print_error("statsThread: cannot parse data in queue!")
            errors += 1
        finally:
            if errors > 10:
                print_warn("statsThread: too many errors, stopping now!")
                break

def diffsThread(queue, fout):
    print_log("start diffsThread")
    errors = 0
    calls = 0
    data0 = queue.get()
    if data0 == 'DONE':
        return
    while True:
        data1 = queue.get()
        if data1 == 'DONE':
            break
        try:
            ts0 = data0[0]
            ts1 = data1[0]
            mt0 = data0[1]
            mt1 = data1[1]
            st0 = data0[2]
            st1 = data1[2]
            pt0 = data0[3]
            pt1 = data1[3]
            if (mt0==mt1) and (st0==st1):
                calls += 1
                print_info("diffThread, call %d" % calls)
                diffs = getDiffs(pt0, pt1)
                outputDiffs(fout, ts0, ts1, mt0, st0, diffs)
        except:
            print_error("diffsThread: cannot parse data in queue!")
            errors += 1
        finally:
            if errors > 10:
                print_warn("diffsThread: too many errors, stopping now!")
                break
        data0 = data1

def outputStats (fout, ts, mt, st, pl, pi, pb, pm):
    global stats_print
    k = str(ts)+'_'+mt+'_'+st
    if k not in stats_print:
        stats_print.append(k)
        output = 'STATS;'+str(ts)+';'+mt+';'+st+';'
        for p in sorted(pl.keys()):
            output += str(pl[p])+';'
        output += str(pi)+';'
        output += str(pb)+';'
        output += str(pm)
        if fout:
            fn = mt+'.'+st+'.stats.csv'
            with open(fn, "a+") as f:
                f.write(output+'\n')
        else:
            print(output)
            sys.stdout.flush()

def outputDiffs(fout, ts0, ts1, mt, st, diffs):
    output = 'DIFFS;'+str(ts0)+';'+str(ts1)+';'+mt+';'+st+';'
    output += ';'.join(str(x) for x in diffs)
    if fout:
        fn = mt+'.'+st+'.diffs.csv'
        with open(fn, "a+") as f:
                f.write(output+'\n')
    else:
        print(output)
        sys.stdout.flush()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    parser.add_argument('-t', '--threads',      help='Use threads for parallel and faster processing.', action='store_true', default=False)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--single',        help='Process a single file, results are printed to STDOUT.')
    group.add_argument('-b', '--bulk',          help='Process a bunch of files in given directory (optional recursive).s')
    parser.add_argument('-r', '--recursive',    help='Search directories recursivly if in bulk mode.', action='store_true')
    parser.add_argument('-f', '--file',         help='Write results to files stats.csv and diffs.csv in working directory.', action='store_true', default=False)
    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']

    global warning
    warning   = args['warning']

    global logging
    logging   = args['logging']

    writedata = args['file']
    recursive = args['recursive']
    threads   = args['threads']

    bulk      = args['bulk']
    single    = args['single']

    start_time = datetime.now()

    print_log("START: " + start_time.strftime('%Y-%m-%d %H:%M:%S'))
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

        all_files.sort()
        print_log("matching files: %d" % (len(all_files)))

        if threads:
            stats_queue = Queue(queue_limit)
            diffs_queue = Queue(queue_limit)
            stats_p = Process(target=statsThread, args=((stats_queue),writedata,))
            diffs_p = Process(target=diffsThread, args=((diffs_queue),writedata,))

            stats_p.daemon = True
            diffs_p.daemon = True
            stats_p.start()
            diffs_p.start()
            inputThread(all_files, stats_queue, diffs_queue)
            stats_p.join()
            diffs_p.join()

        else:
            work_load = []
            for i in range(len(all_files)-1):
                work_load.append([all_files[i],all_files[i+1]])
            for w in work_load:
                singleWorker(writedata, w)

    elif single:
        print_log("mode: single")
        if os.path.isfile(single):
            ts0, mt0, st0 = parseFilename(os.path.abspath(single))
            pt0 = getPtree(single)
            pl0, pi0, pb0, pm0 = getStats(pt0)
            outputStats(writedata, ts0,mt0,st0,pl0,pi0,pb0,pm0)
        else:
            print_error("File not found (%s)!" % (single))
    else:
        print_error("Missing parameter: choose bulk or single mode!")
        exit(1)

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))


if __name__ == "__main__":
    main()
