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

def retrieve_postgres(dbconnstr, maxts='2005-01-03', mt='routeviews', st='route-views.wide'):
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("retrieve_postgres: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()

    query_datasets = "SELECT id, ts FROM t_datasets WHERE ts < \'%s\' AND maptype = \'%s\' AND subtype = \'%s\' ORDER BY ts"
    query_origins = "SELECT p.prefix, o.asn FROM (SELECT * FROM t_origins WHERE dataset_id = \'%s\') AS o LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id"

    datasets = OrderedDict()
    try:
        query = query_datasets % (maxts,mt,st)
        cur.execute(query)
        rs = cur.fetchall()
        datasets = OrderedDict((str(rs[i][0]), str(rs[i][1])) for i in range(len(rs)))
    except Exception, e:
        print_error("QUERY: %s ; failed with: %s" % (query, e.message))
        con.rollback()

    results = dict()
    results['ts'] = list()

    cnt = 0
    for did in datasets:
        print_info("RUN %s, processing did: %s, dts: %s" % (str(cnt+1), did, datasets[did]))
        results['ts'].append(datasets[did])
        try:
            query = query_origins % did
            cur.execute(query)
            rs = cur.fetchall()
            for i in range(len(rs)):
                pfx = str(rs[i][0])
                asn = int(rs[i][1])
                if pfx not in results:
                    results[pfx] = list()
                    j = 0
                    while j < cnt:
                        results[pfx].append([0])
                        j = j+1
                if len(results[pfx]) < cnt:
                    for k in range(len(results[pfx]),cnt):
                        results[pfx].append([0])
                if len(results[pfx]) == cnt:
                    results[pfx].append([asn])
                else:
                    results[pfx][cnt].append(asn)
        except Exception, e:
            print_error("QUERY: %s ; failed with: %s" % (query, e.message))
            con.rollback()
        cnt = cnt+1
    return results

def process_data(data):
    results = dict()
    for pfx in data:
        # ignore timestamp field
        if pfx == 'ts':
            continue
        #print_info("processing prefix: %s" % (pfx))
        results[pfx] = list()
        ts0 = data['ts'][0]
        as0 = data[pfx][0][0]
        ts1 = ts0
        as1 = as0
        for i in range(2,len(data[pfx])):
            ts2 = data['ts'][i]
            as2 = data[pfx][i][0]
            if as2 != as1:
                origin_ttl = (as0, ts0, ts1)
                results[pfx].append( origin_ttl )
                as0 = as2
                ts0 = ts2
                as1 = as2
                ts1 = ts2
            else:
                ts1 = ts2
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    imode = parser.add_mutually_exclusive_group(required=False)
    imode.add_argument('-j', '--json',          help='Write data to JSON file.',    default=False)
    imode.add_argument('-c', '--couchdb',       help='Write data to CouchDB.',      default=False)
    imode.add_argument('-m', '--mongodb',       help='Write data to MongoDB.',      default=False)
    imode.add_argument('-p', '--postgres',      help='Write data to PostgresqlDB.', default=False)
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

    if args['postgres']:
        res = retrieve_postgres(args['postgres'])
        out = process_data(res)
        print(json.dumps(out, sort_keys=True, indent=4, separators=(',', ': ')))
    else:
        print_error('No valid data source found!')
    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))

if __name__ == "__main__":
    main()
