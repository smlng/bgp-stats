#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import re
import sys
import json
import psycopg2

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

def origin_ttl_postgres(dbconnstr, outqeue,
                        mints='2005-01-01', maxts='2005-01-03',
                        mt='routeviews', st='route-views.wide'):
    print_log("CALL origin_ttl_postgres")
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("retrieve_postgres: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()

    query_datasets = "SELECT id, ts FROM t_datasets WHERE ts >= '%s' " \
                     "AND ts < '%s' AND maptype = '%s' " \
                     "AND subtype = '%s' ORDER BY ts"
    query_origins = "SELECT p.prefix, o.asn FROM " \
                    "(SELECT * FROM t_origins WHERE dataset_id = '%s') AS o " \
                    "LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id"

    datasets = OrderedDict()
    try:
        query = query_datasets % (mints,maxts,mt,st)
        cur.execute(query)
        rs = cur.fetchall()
        datasets = OrderedDict((row[0], row[1]) for row in rs)
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()

    origins = dict()
    cnt = 0
    for did in datasets:
        cnt = cnt+1
        print_info("RUN %s, processing did: %s, dts: %s" %
                    (cnt, did, datasets[did]))
        ts_str = datasets[did])
        ts = (datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S") -
                    datetime(1970, 1, 1)).total_seconds()
        # get origins of dataset
        try:
            query = query_origins % did
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
                origins[pfx][asn] = (ts_str,ts_str)
            else:
                val = origins[pfx][asn]
                origins[pfx][asn] = (val[0],ts_str)
            # check prefix origin association, output and delete old ones
            for pfx in origins:
                for asn in origins[pfx]:
                    val = origins[pfx][asn]
                    if val[1] != ts_str:
                        ts0 = (datetime.strptime(val[0], "%Y-%m-%d %H:%M:%S") -
                                    datetime(1970, 1, 1)).total_seconds()
                        ts1 = (datetime.strptime(val[1], "%Y-%m-%d %H:%M:%S") -
                                    datetime(1970, 1, 1)).total_seconds()
                        ttl = ts1 - ts0
                        res = (pfx,asn,val[0],val[1],ttl)
                        outqeue.put(res)
                        del origins[pfx][asn]
    return True

def outputThread(outqeue, opts):
    print_log("CALL outputThread")
    oid = 0
    # init output
    if opts[0] == 'json':
        fout = opts[1]
        if not fout.lower().endswith('.gz'):
            fout = fout+".gz"
        f = gzip.open(fout, "wb")
        header = ('{'
                  ' "begin" : "%s",'
                  ' "until" : "%s",'
                  ' "maptype" : "%s",'
                  ' "subtype" : "%s",'
                  ' "origin_ttls" : [\n')
        f.write(header)
    elif opts[0] == 'postres':
        try:
            con = psycopg2.connect(dbconnstr)
        except Exception, e:
            print_error("retrieve_postgres: connecting to database")
            print_error("failed with: %s" % ( e.message))
            sys.exit(1)
        cur = con.cursor()
        sql_insert = ("INSERT INTO t_origin_ttl "
                      "(ts_begin, ts_until, maptype, subtype)"
                      " VALUES %s, %s, %s, %s RETURNING id")
        sql insert_data = ("INSERT INTO t_origin_ttl_data "
                           "VALUES %s,%s,%s,%s,%s,%s")
        query_prefix = "SELECT id FROM t_prefixes WHERE prefix = %s"
        try:
            cur.execute(sql_insert, opts[2:6])
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
        if not fout.lower().endswith('.gz'):
            fout = fout+".gz"
        f = gzip.open(fout, "wb")
        header = ("# begin: %s\n"
                  "# until: %s\n"
                  "# maptype: %s\n"
                  "# subtype: %s\n"
                  "Prefix;ASN;ts0;ts1,ttl\n" % (opts[2:6]))
        f.write(header)

    # output queue data
    first = True
    while True:
        odata = outqeue.get()
        if (odata == 'DONE'):
            break
        if opts[0] == 'json':
            if not first:
                f.write(",\n")
            else:
                first = False
            f.write(json.dumps(odata))
        elif opts[0] == 'postgres':
            pid = 0
            pfx = odata[0]
            if pfx in prefix_ids:
                pid = prefix_ids[pfx]
            else:
                try:
                    
            try:
                cur.execute(sql_insert_data,
                            [oid,pid,odata[1],odata[2],odata[3],
                                              odata[4],odata[5]])
                con.commit()
            except Exception, e:
                print_error("INSERT t_origin_ttl failed with: %s" % (e.message))
                con.rollback()
        elif opts[0] == 'mongodb':
            print_error("WTF? Still not implemented yet! How'd u get here?")
            sys.exit(1)
        else:
            f.write(';'.join(str(x) for x in odata) + "\n")

    # finalize output
    if opts[0] == 'json':
        footer = (' ]\n}')
        f.write(footer)
        f.close()
    elif opts[0] == 'csv':
        f.close()
    # and done
    return True

def output(data, opts, meta):
    if opts[0] == 'json':
        output_json(data,opts[1])
    elif opts[0] == 'postres':
        output_postgres(data, opts[1], meta)
    elif opts[0] == 'mongodb':
        output_mongodb(data, opts[1], meta)
    else:
        output_csv(data,opts[1])

def output_csv(data, fout):
    f = sys.stdout
    if fout:
        try:
            if not fout.lower().endswith('.gz'):
                fout = fout+".gz"
            f = gzip.open(fout, "ab")
        except:
            print_error("Failed open file %s, using STDOUT instead" % (fout))
            f = sys.stdout
    for d in data:
        print(';'.join(str(x) for x in d), file=f)

def ouptut_json(data, fout):
    f = sys.stdout
    if fout:
        try:
            if not fout.lower().endswith('.gz'):
                fout = fout+".gz"
            f = gzip.open(fout, "ab")
        except:
            print_error("Failed open file %s, using STDOUT instead" % (fout))
            f = sys.stdout
    print (json.dumps(data), file=f)

def output_postgres(data, dbconnstr, meta):
    insert_origin_ttl = "INSERT INTO t_origin_ttl " \
                        "(ts_begin, ts_until, maptype, subtype) " \
                        "VALUES (%s, %s, %s, %s) RETURNING id"
    insert_origin_ttl_data = "INSERT INTO"
    pass

def output_mongodb(data, dbconnstr, meta):
    print_error("Not implemented yet!")
    pass

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
                        type=valid_date, default="2005-01-02")
    parser.add_argument('-t', '--type',
                        help='Type of data source (show all: ?).',
                        type=str, default="routeviews")
    parser.add_argument('-s', '--subtype',
                        help='Subtype of data source (show all: ?)',
                        type=str, default="route-views.wide")
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
            oopts = ('postgres', args['postres'],
                     begin, until, maptype, subtype)
        elif args['mongodb']:
            oopts = ('mongodb', args['mongodb'],
                     begin, until, maptype, subtype)

    # start output process to
    output_queue = Queue()
    output_p = Process(target=outputThread,
                       args=(output_queue,oopts))
    output_p.start()

    if args['postgres']:
        done = origin_ttl_postgres(args['postgres'], output_queue,
                                   begin, until, maptype, subtype)
    else:
        print_error('No valid data source found!')

    output_queue.put('DONE')
    output_p.join()

    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))

if __name__ == "__main__":
    main()
