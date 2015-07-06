#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import radix
import re
import sys
import multiprocessing as mp

from bz2 import BZ2File
from datetime import datetime, timedelta
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
num_ips_valid_all = len(IPSet(0.0.0.0/0) - reserved_ipv4)

## helper function ##

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
    query_origins = ("SELECT p.prefix, o.asn FROM "
                     "(SELECT * FROM %s WHERE dataset_id = '%s') AS o "
                     "LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id")
    ym_str = ts_str.strftime("%Y_%m")
    table = "t_origins_"+ym_str
    ptree = radix.Radix()
    # get origins of dataset
    try:
        query = query_origins % (table, did)
        cur.execute(query)
        rs = cur.fetchall()
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()
    else:
        # update timestamps of prefix origin association
        for row in rs:
            prefix = str(row[0])
            origin = int(row[1])
            if prefix not in ptree:
                pnode = ptree.add(prefix)
                pnode.data['asn'] = list()
            pnode.data['asn'].append(origin)
    return ptree

def get_stat(pt):
    print_log("CALL get_stat")
    ips = IPSet(pt.prefixes())
    num_ips_valid = len(ips - reserved_ipv4)
    num_ips_bogus = num_ips_all - num_ips-valid

def get_diff(pt0, pt1):
    print_log("CALL get_diff")
    ips_agg = IPSet()
    ips_deagg = IPSet()
    pt0IPs = IPSet(pt0.prefixes()) - reserved_ipv4
    pt1IPs = IPSet(pt1.prefixes()) - reserved_ipv4
    num_ips_new = len(pt1IPs - pt0IPs)
    num_ips_del = len(pt0IPs - pt1IPs)
    num_pfx_new = len(set(pt1.prefixes()) - set(pt0.prefixes()))
    num_pfx_del = len(set(pt0.prefixes()) - set(pt1.prefixes()))
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
    ret = [len(pt0IPs), len(pt1IPs), num_ips_new, num_ips_del,
           num_ips_changed, num_ips_agg, num_ips_deagg]
    return ret

def worker(dbconnstr, inqueue, outqueue):
    print_log ("START get_diff")

    for data in iter(inqueue.get, 'DONE'):
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
            output = (did0, did1, stat0, stat1, diffs)
            outqueue.put(output)
        except Exception, e:
            print_error("%s failed with: %s" %
                        (mp.current_process().name, e.message))
    return True

def output(dbconnstr, queue):
    pass

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
    mgr = mp.Manager()
    input_queue = mgr.Queue()
    output_queue = mgr.Queue()
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
    try:
        query = query_datasets % (mints,maxts,mt,st)
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
    for w in xrange(workers):
        p = mp.Process(target=worker,
                       args=(dbconnstr,input_queue,output_queue))
        p.start()
        processes.append(p)
        input_queue.put('DONE')
    # start output process to
    output_p = mp.Process(target=output,
                          args=(dbconnstr,output_queue))
    output_p.start()

    for p in processes:
        p.join()

    output_queue.put('DONE')
    output_p.join()

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))


if __name__ == "__main__":
    main()
