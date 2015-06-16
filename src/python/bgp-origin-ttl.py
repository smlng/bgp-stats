#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import re
import sys
import json
import psycopg2

from time import sleep
from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import OrderedDict
from multiprocessing import Process, Queue, cpu_count, current_process

verbose = False
warning = False
logging = False

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

def origin_ttl_postgres(dbconnstr, outqeue, mints, maxts, mt, st):
    print_log("CALL origin_ttl_postgres (%s,%s,%s,%s)" % (mints,maxts,mt,st))
    print_info(dbconnstr)
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
    query_origins = ("SELECT p.prefix, o.asn FROM "
                     "(SELECT * FROM %s WHERE dataset_id = '%s') AS o "
                     "LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id")

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
    origins = dict()
    cnt = 0
    for did in datasets:
        cnt = cnt+1
        print_info("RUN %s, processing did: %s, dts: %s" %
                    (cnt, did, datasets[did]))
        ts_str = datasets[did]
        ym_str = ts_str.strftime("%Y_%m")
        table = "t_origins_"+ym_str
        #ts = (datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S") -
        #            datetime(1970, 1, 1)).total_seconds()
        ts = (ts_str - datetime(1970, 1, 1)).total_seconds()
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
                pfx = str(row[0])
                asn = int(row[1])
                if pfx not in origins:
                    origins[pfx] = dict()
                if asn not in origins[pfx]:
                    val = (ts_str,ts_str)
                    origins[pfx][asn] = val
                else:
                    old = origins[pfx][asn]
                    val = (old[0],ts_str)
                    origins[pfx][asn] = val
            # check prefix origin association, output and delete old ones
            delorigin = list()
            for pfx in origins:
                for asn in origins[pfx]:
                    val = origins[pfx][asn]
                    if val[1] != ts_str:
                        ts0 = (val[0] - datetime(1970, 1, 1)).total_seconds()
                        ts1 = (val[1] - datetime(1970, 1, 1)).total_seconds()
                        ttl = int(ts1 - ts0)
                        if ttl > 0:
                            res = (pfx,asn,str(val[0]),str(val[1]),ttl)
                            outqeue.put(res)
                        dl = (pfx, asn)
                        delorigin.append(dl)
            for d in delorigin:
                pfx = d[0]
                asn = d[1]
                del origins[pfx][asn]
    for pfx in origins:
        for asn in origins[pfx]:
            val = origins[pfx][asn]
            ts0 = (val[0] - datetime(1970, 1, 1)).total_seconds()
            ts1 = (val[1] - datetime(1970, 1, 1)).total_seconds()
            ttl = int(ts1 - ts0)
            if ttl > 0:
                res = (pfx,asn,str(val[0]),str(val[1]),ttl)
                outqeue.put(res)
    return True

def output_thread(outqeue, opts):
    print_log("CALL output_thread")
    oid = 0
    # init output
    if opts[0] == 'json':
        fout = opts[1]
        f = sys.stdout
        if fout and (not fout.lower().endswith('.gz')):
            fout = fout+".gz"
            f = gzip.open(fout, "wb")
        header = ('{'
                  ' "begin" : "%s",'
                  ' "until" : "%s",'
                  ' "maptype" : "%s",'
                  ' "subtype" : "%s",'
                  ' "origin_ttls" : [\n' % (opts[2:6]))
        f.write(header)
        f.flush()
    elif opts[0] == 'postgres':
        dbconnstr = opts[1]
        try:
            con = psycopg2.connect(dbconnstr)
        except Exception, e:
            print_error("retrieve_postgres: connecting to database")
            print_error("failed with: %s" % ( e.message))
            sys.exit(1)
        cur = con.cursor()
        insert_origin = ("INSERT INTO t_origin_ttl "
                      "(ts_begin, ts_until, maptype, subtype)"
                      " VALUES (%s, %s, %s, %s) RETURNING id")
        insert_data = ("INSERT INTO t_origin_ttl_data "
                           "VALUES (%s,%s,%s,%s,%s,%s)")
        query_prefix = "SELECT id FROM t_prefixes WHERE prefix = %s"
        insert_prefix = "INSERT INTO t_prefixes (prefix) VALUES (%s) RETURNING id"
        try:
            cur.execute(insert_origin, [opts[2].strftime('%Y-%m-%d'),
                                        opts[3].strftime('%Y-%m-%d'),
                                        opts[4],opts[5]])
            con.commit()
            oid = cur.fetchone()[0]
        except Exception, e:
            print_error("INSERT t_origin_ttl failed with: %s" % (e.message))
            con.rollback()
            sys.exit(1)
        else:
            if oid == 0:
                print_error("No valid origin_ttl id!")
                sys.exit(1)
        # get all prefixes already in database
        query_all_prefixes = "SELECT prefix, id FROM t_prefixes"
        prefix_ids = dict()
        try:
            cur.execute(query_all_prefixes)
            pfx = cur.fetchall()
            prefix_ids = dict((pfx[i][0], pfx[i][1]) for i in range(len(pfx)))
        except Exception, e:
            print_error("QUERY t_prefixes (1) failed with: %s" % (e.message))
            con.rollback()
    elif opts[0] == 'mongodb':
        print_error("Not implemented yet! How did you even get here?!")
        sys.exit(1)
    else: # csv
        fout = opts[1]
        f = sys.stdout
        if fout and (not fout.lower().endswith('.gz')):
            fout = fout+".gz"
            f = gzip.open(fout, "wb")
        header = ("# begin: %s\n"
                  "# until: %s\n"
                  "# maptype: %s\n"
                  "# subtype: %s\n"
                  "Prefix;ASN;ts0;ts1,ttl\n" % (opts[2:6]))
        f.write(header)
        f.flush()

    # output queue data
    first = True
    while True:
        odata = outqeue.get()
        if (odata == 'DONE'):
            print_log("EXIT output_thread")
            break
        if opts[0] == 'json':
            if not first:
                f.write(",\n")
            else:
                first = False
            f.write(json.dumps(odata))
            f.flush()
        elif opts[0] == 'postgres':
            pid = 0
            pfx = odata[0]
            if pfx in prefix_ids:
                pid = prefix_ids[pfx]
            else:
                try:
                    cur.execute(insert_prefix, [pfx])
                    con.commit()
                    pid = cur.fetchone()
                except Exception, e:
                    print_error("INSERT t_prefixes failed with: %s" % (e.message))
                    con.rollback()

            if pid > 0:
                try:
                    cur.execute(insert_data,
                                [oid,pid,odata[1],odata[2],odata[3],odata[4])
                    con.commit()
                except Exception, e:
                    print_error("INSERT t_origin_ttl failed with: %s" % (e.message))
                    con.rollback()
            else:
                print_warn("Invalid ID for prefix %s" % (pfx))

        elif opts[0] == 'mongodb':
            print_error("WTF? Still not implemented yet! How'd u get here?")
            sys.exit(1)
        else:
            f.write(';'.join(str(x) for x in odata) + "\n")
            f.flush()

    # finalize output
    if opts[0] == 'json':
        footer = (' ]\n}')
        f.write(footer)
        if opts[1]:
            f.close()
    elif opts[0] == 'csv':
        f.flush()
        if opts[1]:
            f.close()
    # and done
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',
                        help='print logging.',
                        action='store_true')
    parser.add_argument('-w', '--warning',
                        help='print warnings.',
                        action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='print everything.',
                        action='store_true')

    imode = parser.add_mutually_exclusive_group(required=True)
    imode.add_argument('-m', '--mongodb',
                        help='Read from MongoDB.',
                        type=str)
    imode.add_argument('-p', '--postgres',
                        help='Read from PostgresqlDB.',
                        type=str)

    omode = parser.add_mutually_exclusive_group(required=False)
    omode.add_argument('-c', '--csv',
                        help='Output data as CSV.',
                        action='store_true')
    omode.add_argument('-j', '--json',     help='Output data as JSON.',
                        action='store_true')
    omode.add_argument('-d', '--database',
                        help="Store data into database (same as input).",
                        action='store_true')

    parser.add_argument('-f', '--file',
                        help='Write data to file',
                        default=False)
    parser.add_argument('-b', '--begin',
                        help='Begin date (inclusive), format: yyyy-mm-dd',
                        type=valid_date, default="2005-01-01")
    parser.add_argument('-u', '--until',
                        help='Until date (exclusive), format: yyyy-mm-dd',
                        type=valid_date, default="2005-02-01")
    parser.add_argument('-t', '--type',
                        help='Type of data source (show all: ?).',
                        type=str, default="routeviews")
    parser.add_argument('-s', '--subtype',
                        help='Subtype of data source (show all: ?)',
                        type=str, default="route-views.eqix")
    args = vars(parser.parse_args())

    # output settings
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

    # output options, tupel: how, where, ts0, ts1, type, subtype
    oopts = ('csv', args['file'], begin, until, maptype, subtype)
    if args['json']:
        oopts = ('json', args['file'], begin, until, maptype, subtype)
    elif args['database']:
        if args['postgres']:
            oopts = ('postgres', args['postgres'],
                     begin, until, maptype, subtype)
        elif args['mongodb']:
            oopts = ('mongodb', args['mongodb'],
                     begin, until, maptype, subtype)

    # start output process to
    output_queue = Queue()

    if args['postgres']:
        main_p = Process(target=origin_ttl_postgres,
                         args=(args['postgres'], output_queue,
                               begin, until, maptype, subtype))
    else:
        print_error('No valid data source found!')

    main_p.start()
    output_p = Process(target=output_thread,
                       args=(output_queue, oopts))
    output_p.start()
    main_p.join()
    output_queue.put('DONE')
    output_p.join()
    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))

if __name__ == "__main__":
    main()
