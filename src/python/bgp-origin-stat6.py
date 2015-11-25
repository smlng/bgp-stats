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
import math

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

nonunicast_ipv6 = IPSet (['::1/128','::/128',         # Node-Scoped Unicast
                        '::FFFF:0:0/96',            # IPv4-Mapped Addresses
                        'fe80::/10',                # Link-Scoped Unicast
                        'fc00::/7',                 # Unique-Local
                        '2001:0000::/23',           # IANA testing
                        '2001:db8::/32',            # Documentation Prefix
                        '5f00::/8','3ffe::/16',     # formerly 6bone testing
                        'ff00::/8',                 # multicast
                    ])
special_ipv6 = IPSet  (['2002::/16',                # 6to4
                        '2001::/32',                # Toredo
                        '2001:10::/28',             # ORCHID
                    ])

reserved_ipv6 = nonunicast_ipv6 - special_ipv6

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
def get_origins(dbconnstr, did, ts_str):
    print_log ("CALL get_origins (%s, %s, %s)" % (dbconnstr, did, ts_str))
    query_origins = ("SELECT p.prefix, o.asn FROM "
                     "(SELECT * FROM %s WHERE dataset_id = '%s') AS o "
                     "LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id")
    ym_str = ts_str.strftime("%Y_%m")
    table = "t_origins_"+ym_str
    ptree = dict()
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("get_origins: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()
    # get origins of dataset
    try:
        print_info("get_origins: execute query")
        query = query_origins % (table, did)
        cur.execute(query)
        rs = cur.fetchall()
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()
    else:
        print_info("get_origins: process response")
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
    num_ips_all = 0
    ips_valid = 0
    num_ips_valid = 0
    num_ips_bogus = 0
    ipspace = 0
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
    for i in range(128): # init 1-128 with 0
        pl_dict[i+1] = 0
    for pl in pfxlen:
        pl_dict[pl] = len(pfxlen[pl])
    str_pfx_len = ','.join(str(pl_dict[i+1]) for i in range(128))
    ret = [num_asn,num_ips_valid, num_ips_bogus, ipspace,
           num_pfx, num_pfx_moas, str_pfx_len]
    return ret

def worker(dbconnstr, queue):
    print_log ("START worker")

    for data in iter(queue.get, 'DONE'):
        try:
            did = data[0]
            ts  = data[1]
            origins = get_origins(dbconnstr, did, ts)
            print_info ("%s get_origins done ..." % (mp.current_process().name))
            stat = get_stat(origins)
            print_info ("%s get_stat done ..." % (mp.current_process().name))
            odata = list()
            odata.append(did)
            odata.extend(stat)
            output(dbconnstr, odata)
            print_info ("%s output done ..." % (mp.current_process().name))
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
    insert_stat = "INSERT INTO t_origin_stats VALUES (%s,%s,%s,%s,%s,%s,%s,'%s')"
    sql_insert = insert_stat % tuple(odata)
    try:
        print_info("output: insert stats")
        cur.execute(sql_insert)
        con.commit()
    except Exception, e:
        print_error("INSERT: %s ; failed with: %s" % (sql_insert, e.message))
        con.rollback()
    else:
        print_info ("STAT: " + ';'.join( str(x) for x in odata))
    return True

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
    print_info ("fill input queue")
    for i in datasets.items():
        input_queue.put(i)
    # start workers
    print_info("start workers")
    processes = []
    for w in xrange(workers):
        p = mp.Process(target=worker,
                       args=(dbconnstr,input_queue))
        p.start()
        processes.append(p)
        input_queue.put('DONE')
    print_info("wait for workers")
    for p in processes:
        p.join()

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))


if __name__ == "__main__":
    main()
