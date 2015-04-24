#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import radix
import re
import sys

from bz2 import BZ2File
from datetime import datetime, timedelta
from multiprocessing import Process, Queue, cpu_count
from netaddr import IPSet, IPNetwork

# own imports
import mrtx

verbose = False
warning = False
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

def getDiffs (pt0, pt1):
    print_log("call getDiffs")
    ips_agg = IPSet()
    ips_deagg = IPSet()
    pt0IPs = IPSet(pt0.prefixes()) - reserved_ipv4
    pt1IPs = IPSet(pt1.prefixes()) - reserved_ipv4
    num_ips_new = len(pt1IPs - pt0IPs)
    num_ips_del = len(pt0IPs - pt1IPs)
    for pn0 in pt0:
        ipn0 = IPNetwork(pn0.prefix)
        if ipn0 not in reserved_ipv4:
            pn1 = pt1.search_best(pn0.network)
            if (pn1 != None) and (pn0.prefix != pn1.prefix):
                print_info("pn0: %s, pn1: %s" % (pn0.prefix, pn1.prefix))
                diff = pn0.prefixlen - pn1.prefixlen
                if diff > 0: # aggregate
                    try:
                        ips_agg = ips_agg | IPSet([pn0.prefix])
                    except:
                        print_error("Failed to add prefix (%s) to ips_agg!" % (pn0.prefix))
                if diff < 0: # deaggregate
                    try:
                        ips_deagg = ips_deagg | IPSet([pn0.prefix])
                    except:
                        print_error("Failed to add prefix (%s) to ips_deagg!" % (pn0.prefix))
            
    num_ips_agg = len(ips_agg)
    num_ips_deagg = len(ips_deagg)
    num_ips_changed = num_ips_agg + num_ips_deagg
    ret = [len(pt0IPs), len(pt1IPs), num_ips_new, num_ips_del, num_ips_changed, num_ips_agg, num_ips_deagg]
    return ret

def singleWorker(wd, fin):
    print_log("call singleWorker(\n\t fin0: %s,\n\t fin1: %s)" % (fin[0],fin[1]))

    ts0, mt0, st0 = parseFilename(fin[0])
    ts1, mt1, st1 = parseFilename(fin[1])
    if (mt0==mt1) and (st0==st1):
        pt0 = loadPtree(fin[0])
        pt1 = loadPtree(fin[1])
        diffs = getDiffs(pt0, pt1)
    outputDiffs(wd,ts0,ts1,mt0,st0,diffs)

def diffsThread(inq,outq):
    print_log("start diffsThread")

    for fin in iter(inq.get, 'DONE'):
        try:
            ts0, mt0, st0 = parseFilename(fin[0])
            ts1, mt1, st1 = parseFilename(fin[1])
            if (mt0==mt1) and (st0==st1):
                pt0 = loadPtree(fin[0])
                pt1 = loadPtree(fin[1])
                diffs = getDiffs(pt0, pt1)
                outq.put([ts0,ts1,mt0,st0,diffs])
        except Exception, e:
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return True
    
def outputThread(outq, outf):
    while True:
        odata = outq.get()
        if (odata == 'DONE'):
            break
        try:
            outputDiffs(outf,odata[0],odata[1],odata[2],odata[3],odata[4])
        except Exception, e:
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return True

def outputDiffs(fout, ts0, ts1, mt, st, diffs):
    output = str(ts0)+';'+str(ts1)+';'+mt+';'+st+';'
    output += ';'.join(str(x) for x in diffs)
    if fout:
        with open(fout, "a+") as f:
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
    parser.add_argument('-n', '--numthreads',   help='Set number of threads.', type=int, default=None)
    parser.add_argument('-r', '--recursive',    help='Search directories recursivly if in bulk mode.', action='store_true')
    parser.add_argument('-f', '--file',         help='Write results to file.', default=None)
    parser.add_argument('path',                 help='Path to data.')

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
    workers   = args['numthreads']
    if not workers:
        workers = cpu_count() / 2

    path = args['path']

    start_time = datetime.now()

    print_log("START: " + start_time.strftime('%Y-%m-%d %H:%M:%S'))

    if not (os.path.isdir(path)):
        print_error("Invalid path for processing!")
        exit(1)

    all_files = []
    if recursive:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in [f for f in filenames if (re_file_rv.match(f) or re_file_rr.match(f))]:
                all_files.append(os.path.join(dirpath, filename))
    else:
        for filename in [f for f in os.listdir(path) if (re_file_rv.match(f) or re_file_rr.match(f))]:
            all_files.append(os.path.join(path, filename))

    all_files.sort()
    print_log("matching files: %d" % (len(all_files)))

    if threads:
        input_queue = Queue()
        output_queue = Queue()
        processes = []
        # fill input queue
        for i in range(len(all_files)-1):
            input_queue.put([all_files[i],all_files[i+1]])
        # start workers to calc stats
        for w in xrange(workers):
            p = Process(target=diffsThread, args=(input_queue,output_queue))
            p.start()
            processes.append(p)
            input_queue.put('DONE')
        # start output process to 
        output_p = Process(target=outputThread, args=(output_queue,writedata))
        output_p.start()

        for p in processes:
            p.join()

        output_queue.put('DONE')
        output_p.join()
    else:
        for i in range(len(all_files)-1):
            singleWorker(writedata, [all_files[i],all_files[i+1]])

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))


if __name__ == "__main__":
    main()