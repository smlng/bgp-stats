#!/usr/bin/python

#### workflow ####
# 1. get all datatsets within date range
# 2. put dataset pairs into input_queue
# 3. start worker threads with input_queue
# 3a. load dataset into prefix trees pt0 and pt1
# 3.b calc stats for prefix trees pt0 and pt1
# 3.c calc diffs for prefix treees (pt0,pt1)
# 4. start output thread and write results back to database
##################s

from __future__ import print_function

import argparse
import gzip
import os
import psycopg2
import radix
import re
import sys
import multiprocessing as mp

from collections import OrderedDict
from datetime import datetime, timedelta
from netaddr import IPSet, IPNetwork

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
all_ips_valid = len(IPSet(['0.0.0.0/0']) - reserved_ipv4)

## helper function ##

def prefixlen (prefix):
    try:
        network, length = prefix.split('/')
    except:
        return 32
    else:
        return int(length)

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

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

## public and thread funtions ##
def get_tree(dbconnstr, did, ts_str):
    print_log ("CALL get_tree (%s, %s, %s)" % (dbconnstr, did, ts_str))
    query_origins = ("SELECT p.prefix, o.asn FROM "
                     "(SELECT * FROM %s WHERE dataset_id = '%s') AS o "
                     "LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id")
    ym_str = ts_str.strftime("%Y_%m")
    table = "t_origins_"+ym_str
    ptree = dict()
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("get_tree: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()
    # get origins of dataset
    try:
        print_info("execute query")
        query = query_origins % (table, did)
        cur.execute(query)
        rs = cur.fetchall()
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()
    else:
        print_info("process response")
        # update timestamps of prefix origin association
        for row in rs:
            prefix = str(row[0])
            origin = int(row[1])
            if prefix not in ptree:
                ptree[prefix] = list()
            if origin not in ptree[prefix]:
                ptree[prefix].append(origin)
    return ptree

def get_stat(pt):
    print_log("CALL get_stat")
    ips = IPSet(pt.keys())
    num_ips_all = len(ips)
    num_ips_valid = len(ips - reserved_ipv4)
    num_ips_bogus = num_ips_all - num_ips_valid
    ipspace = num_ips_valid / all_ips_valid
    pfxlen = dict()
    asn = set()
    num_pfx_moas = 0
    # eval prefix tree
    for p in pt:
        pl = prefixlen(p)
        for a in pt[p]:
            asn.add(a)
        if len(pt[p]) > 1:
            num_pfx_moas += 1
        if pl not in pfxlen:
            pfxlen[pl] = list()
        pfxlen[pl].append(p)
    num_asn = len(asn)
    num_pfx = len(pt.keys())
    # prefix and ip results
    pl_dict = dict()
    for i in range(32): # init with all 0
        pl_dict[i+1] = 0
    for pl in pfxlen:
        pl_dict[pl] = len(pfxlen[pl])
    str_pfx_len = str(pl_dict[0])
    for i in range(1,32):
        str_pfx_len = str_pfx_len + "," + str(pl_dict[i])
    ret = [num_asn,num_ips_valid, num_ips_bogus, ipspace,
           num_pfx, num_pfx_moas, str_pfx_len]
    return ret

def get_diff(pt0, pt1):
    print_log("CALL get_diff")
    pt0IPs = IPSet(pt0.keys())
    pt1IPs = IPSet(pt1.keys())
    num_ips_new = len(pt1IPs - pt0IPs)
    num_ips_del = len(pt0IPs - pt1IPs)
    num_pfx_new = len(set(pt1.keys()) - set(pt0.keys()))
    num_pfx_del = len(set(pt0.keys()) - set(pt1.keys()))
    num_pfx_mod = 0
    asn0 = set()
    asn1 = set()
    for pn0 in pt0:
        for a in pt0[pn0]:
            asn0.add(a)
        if pn0 in pt1:
            asn_diff = set(pt0[pn0]) ^ set(pt1[pn0])
            if len(asn_diff) > 0:
                num_pfx_mod += 1
    for pn1 in pt1:
        for a in pt1[pn1]:
            asn1.add(a)
    num_asn_new = len(asn1 - asn0)
    num_asn_del = len(asn0 - asn1)
    ret = [num_asn_new, nums_asn_del, num_ips_new, num_ips_del,
           num_pfx_new, num_pfx_del, num_pfx_mod]
    return ret

def worker(dbconnstr, queue):
    print_log ("START worker")

    for data in iter(queue.get, 'DONE'):
        try:
            did0 = data[0]
            ts0  = data[1]
            did1 = data[2]
            ts1  = data[3]
            ptree0 = get_tree(dbconnstr, did0, ts0)
            ptree1 = get_tree(dbconnstr, did1, ts1)
            stat0 = get_stat(ptree0)
            stat1 = get_stat(ptree1)
            diffs = get_diff(ptree0, ptree1)
            odata = (did0, did1, stat0, stat1, diffs)
            output(dbconnstr, odata)
        except Exception, e:
            print_error("%s failed with: %s" %
                        (mp.current_process().name, e.message))
    return True

def output(dbconnstr, odata):
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("output: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()
    insert_stats = "INSERT INTO t_origin_stats VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    insert_diffs = "INSERT INTO t_origin_diffs VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    stat0 = list(odata[0])
    stat0.extend(odata[2])
    stat1 = list(odata[1])
    stat1.extend(odata[3])
    diffs = list(odata[0],odata[1])
    diffs.extend(odata[4])
    print_info ("STAT0: " + ';'.join( str(x) for x in stat0))
    print_info ("STAT1: " + ';'.join( str(x) for x in stat1))
    print_info ("DIFFS: " + ';'.join( str(x) for x in diffs))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',
                        help='Ouptut logging.',
                        action='store_true')
    parser.add_argument('-w', '--warning',
                        help='Output warnings.',
                        action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='print everything.',
                        action='store_true')
    parser.add_argument('-p', '--postgres',
                        help='Use PostgresqlDB for input and output.',
                        required=True)
    parser.add_argument('-b', '--begin',
                        help='Begin date (inclusive), format: yyyy-mm-dd',
                        type=valid_date, required=True)
    parser.add_argument('-u', '--until',
                        help='Until date (exclusive), format: yyyy-mm-dd',
                        type=valid_date, required=True)
    parser.add_argument('-t', '--type',
                        help='Type of data source (show all: ?).',
                        type=str, required=True)
    parser.add_argument('-s', '--subtype',
                        help='Subtype of data source (show all: ?)',
                        type=str, required=True)
    parser.add_argument('-n', '--numthreads',
                        help='Set number of threads.',
                        type=int, default=2)
    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging   = args['logging']

    # run
    start_time = datetime.now()
    print_log("START: " + start_time.strftime('%Y-%m-%d %H:%M:%S'))

    begin = args['begin']
    until = args['until']
    maptype = args['type']
    subtype = args['subtype']
    dbconnstr = args['postgres']
    workers   = args['numthreads']
    if not workers:
        workers = mp.cpu_count() / 2

    # prepare some vars
    input_queue = mp.Queue()
    # get all matching datasets
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("origin_ttl_postgres: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()

    query_datasets = ("SELECT id, ts FROM t_datasets WHERE ts >= '%s' "
                      "AND ts < '%s' AND maptype = '%s' "
                      "AND subtype = '%s' ORDER BY ts")
    datasets = OrderedDict()
    query = query_datasets % (begin,until,maptype,subtype)
    try:
        cur.execute(query)
        rs = cur.fetchall()
        datasets = OrderedDict((row[0], row[1]) for row in rs)
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()
    print_log ("FOUND %s datasets." % str(len(datasets)))
    # fill input_queue
    for i in range(0, len(datasets)-1):
        d0 = datasets.items()[i]
        d1 = datasets.items()[i+1]
        data = (d0[0],d0[1],d1[0],d1[1])
        input_queue.put(data)

    # start workers
    processes = []
    for w in xrange(workers):
        p = mp.Process(target=worker,
                       args=(dbconnstr,input_queue))
        p.start()
        processes.append(p)
        input_queue.put('DONE')

    for p in processes:
        p.join()

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))


if __name__ == "__main__":
    main()
