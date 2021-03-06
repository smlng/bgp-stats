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
from netaddr import IPSet

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

existing_data = list()

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
        for o in list(origins):
            if o not in pnode.data['asn']:
                pnode.data['asn'].append(str(o))
        pnode.data['moas'] = len(pnode.data['asn'])
    return ptree

# add num_pfx to stats
def getStats (ptree):
    print_log("call getStats")
    pfxlen = dict()
    asn = dict()
    num_pfx_moas = 0
    # eval prefix tree
    for p in ptree:
        pl = int(p.prefixlen)
        for a in p.data['asn']:
            if a not in asn:
                asn[a] = list()
            asn[a].append(p.prefix)
        if p.data['moas'] > 1:
            num_pfx_moas += 1
        if pl not in pfxlen:
            pfxlen[pl] = list()
        pfxlen[pl].append(p.prefix)

    # asn results
    num_asn = len(asn.keys())
    num_asn_pfx = list()
    num_asn_ips = list()
    for a in asn:
        num_asn_pfx.append(len(asn[a]))
        num_asn_ips.append(len(IPSet(asn[a])))
    # min, max, avg/mean, median
    if len(num_asn_pfx) < 1:
        num_asn_pfx.append(0)
    if len(num_asn_ips) < 1:
        num_asn_ips.append(0)
        
    min_asn_pfx = min(num_asn_pfx)
    max_asn_pfx = max(num_asn_pfx)
    avg_asn_pfx = sum(num_asn_pfx)/len(num_asn_pfx)
    med_asn_pfx = sorted(num_asn_pfx)[int(round(len(num_asn_pfx)/2))]
    min_asn_ips = min(num_asn_ips)
    max_asn_ips = max(num_asn_ips)
    avg_asn_ips = sum(num_asn_ips)/len(num_asn_ips)
    med_asn_ips = sorted(num_asn_ips)[int(round(len(num_asn_ips)/2))]
    
    # prefix and ip results
    pl_dict = dict()
    for i in range(32): # init with all 0
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
    num_pfx = len(ptree.prefixes())

    ret = list()
    for i in range(32):
        ret.append(pl_dict[i+1])
    ret.extend([num_pfx,num_pfx_ips,num_bogus_ips,num_pfx_moas,num_asn])
    ret.extend([min_asn_pfx,max_asn_pfx,avg_asn_pfx,med_asn_pfx])
    ret.extend([min_asn_ips,max_asn_ips,avg_asn_ips,med_asn_ips])
    return ret

stats_header = ["pl01","pl02","pl03","pl04","pl05","pl06","pl07","pl08",
                "pl09","pl10","pl11","pl12","pl13","pl14","pl15","pl16",
                "pl17","pl18","pl19","pl20","pl21","pl22","pl23","pl24",
                "pl25","pl26","pl27","pl28","pl29","pl30","pl31","pl32",
                "num_pfx","num_pfx_ips","num_bog_ips","num_pfx_moa","num_asn",
                "min_asn_pfx","max_asn_pfx","avg_asn_pfx","med_asn_pfx",
                "min_asn_ips","max_asn_ips","avg_asn_ips","med_asn_ips"]

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

def singleWorker(wd, fin):
    print_log("call singleWorker(fin: %s)" % (fin))

    ts0, mt0, st0 = parseFilename(fin)
    if ts0 not in existing_data:
        pt0 = loadPtree(fin)
        stats = getStats(pt0)
        dout = [ts0,mt0,st0]
        dout.extend(stats)
        outputStats(wd,dout)
    else:
        print_info("data set exists, skipping ...")

def statsThread(inq, outq):    
    print_log("start statsThread")

    for fin in iter(inq.get, 'DONE'):
        try:
            ts0, mt0, st0 = parseFilename(fin)
            if ts0 not in existing_data:
                pt0 = loadPtree(fin)
                stats = getStats(pt0)
                dout = [ts0,mt0,st0]
                dout.extend(stats)
                outq.put(dout)
            else:
                print_info("data set exists, skipping ...")
        except Exception, e:
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return True

def outputThread(outq, outf):
    while True:
        odata = outq.get()
        if (odata == 'DONE'):
            break
        try:
            outputStats(outf,odata)
        except Exception, e:
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return True

output_header = ["# timestamp","maptype","subtype"]
output_header.extend(stats_header)

def outputStats (fout, dout):
    output = ';'.join(str(x) for x in dout)
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
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--single',        help='Process a single file, results are printed to STDOUT.')
    group.add_argument('-b', '--bulk',          help='Process a bunch of files in given directory (optional recursive).')
    parser.add_argument('-r', '--recursive',    help='Search directories recursivly if in bulk mode.', action='store_true')
    parser.add_argument('-f', '--file',         help='Write results to file.', default=None)
    args = vars(parser.parse_args())
    
    global verbose
    verbose   = args['verbose']

    global warning
    warning   = args['warning']

    global logging
    logging   = args['logging']

    writedata = args['file']
    if writedata and os.path.isfile(writedata): # read already written data
        with open(writedata, "r") as f:
            global existing_data
            for line in f:
                if line.startswith('#'):
                    continue
                ts = line.split(';')[0].strip()
                try:
                    existing_data.append(int(ts))
                except:
                    print_error("Failure converting timestamp to integer!")
        print_info(existing_data[0])
        print_log("read %d data sets." % (len(existing_data)))

    recursive = args['recursive']
    threads   = args['threads']
    workers   = args['numthreads']
    if not workers:
        workers = cpu_count() / 2

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
            input_queue = Queue()
            output_queue = Queue()
            if len(existing_data) == 0: # write header if no existing data
                output_queue.put(output_header)
            processes = []
            # fill input queue
            for f in all_files:
                input_queue.put(f)
            # start workers to calc stats
            for w in xrange(workers):
                p = Process(target=statsThread, args=(input_queue,output_queue))
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
            for w in all_files:
                singleWorker(writedata, w)

    elif single:
        print_log("mode: single")
        if os.path.isfile(single):
            ts0, mt0, st0 = parseFilename(os.path.abspath(single))
            if ts0 not in existing_data:
                pt0 = loadPtree(single)
                stats = getStats(pt0)
                dout = [ts0,mt0,st0]
                dout.extend(stats)
                outputStats(writedata, output_header)
                outputStats(writedata, dout)
            else:
                print_info("data set exists, skipping ...")
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