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

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def retrieve_postgres(dbconnstr, mints='2005-01-01', maxts='2005-01-03', mt='routeviews', st='route-views.wide'):
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("retrieve_postgres: connecting to database")
        print_error("failed with: %s" % ( e.message))
        sys.exit(1)
    cur = con.cursor()

    query_datasets = "SELECT id, ts FROM t_datasets WHERE ts >= \'%s\' AND ts < \'%s\' AND maptype = \'%s\' AND subtype = \'%s\' ORDER BY ts"
    query_origins = "SELECT p.prefix, o.asn FROM (SELECT * FROM t_origins WHERE dataset_id = \'%s\') AS o LEFT JOIN t_prefixes AS p ON o.prefix_id = p.id"

    datasets = OrderedDict()
    try:
        query = query_datasets % (mints,maxts,mt,st)
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
        # add default asn 0 to all existing prefixes
        for p in results:
            if p != 'ts':
                results[p].append([0])
        try:
            query = query_origins % did
            cur.execute(query)
            rs = cur.fetchall()
            for i in range(len(rs)):
                pfx = str(rs[i][0])
                asn = int(rs[i][1])
                if pfx not in results:
                    results[pfx] = list()
                    for j in range(cnt+1):
                        results[pfx].append([0])
                # replace first asn if 0
                if results[pfx][cnt][0] == 0:
                    results[pfx][cnt][0] = asn
                # add MOAS to prefix
                else:
                    results[pfx][cnt].append(asn)
        except Exception, e:
            print_error("QUERY: %s ; failed with: %s" % (query, e.message))
            con.rollback()
        cnt = cnt+1
    return results

'''
Note: this function does not consider multiple origin AS for a prefix, it uses first in list only
'''
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
            ts1 = data['ts'][i]
            as1 = data[pfx][i][0]
            if as1 != as0:
                origin_ttl = (ts0, ts1, as0)
                results[pfx].append( origin_ttl )
                as0 = as1
                ts0 = ts1
        origin_ttl = (ts0, ts1, as0)
        results[pfx].append( origin_ttl )
    return results

def process_data2(data):
    results = dict()
    for pfx in data:
        # ignore timestamp field
        if pfx == 'ts':
            continue
        #print_info("processing prefix: %s" % (pfx))
        results[pfx] = list()
        ts0 = data['ts'][0]
        as0 = set(data[pfx][0])
        ts1 = ts0
        as1 = as0
        for i in range(2,len(data[pfx])):
            ts1 = data['ts'][i]
            as1 = set(data[pfx][i])
            if as1 != as0:
                origin_ttl = (ts0, ts1, as0)
                results[pfx].append( origin_ttl )
                as0 = as1
                ts0 = ts1
        origin_ttl = (ts0, ts1, as0)
        results[pfx].append( origin_ttl )
    return results

'''
    calculate origin life time, that is duration in [s]econds of prefix <-> origin AS association

    returns list with tupels (prefix, origin-AS, ttl)
'''
def origin_ttl(data):
    results = list()
    for pfx in data:
        for o in data[pfx]:
            ts0 = int((datetime.strptime(o[0], "%Y-%m-%d %H:%M:%S") - datetime(1970, 1, 1)).total_seconds())
            ts1 = int((datetime.strptime(o[1], "%Y-%m-%d %H:%M:%S") - datetime(1970, 1, 1)).total_seconds())
            ttl = ts1 - ts0
            if ttl > 0:
                val = (pfx, o[2], ttl)
                results.append(val)
    return results

def output(data, opts):
    if opts[0] == 'json':
        output_json(data,opts[1])
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='print logging.',          action='store_true')
    parser.add_argument('-w', '--warning',      help='print warnings.',         action='store_true')
    parser.add_argument('-v', '--verbose',      help='print everything.',       action='store_true')
    imode = parser.add_mutually_exclusive_group(required=True)
    imode.add_argument('-m', '--mongodb',       help='Read from MongoDB.',      type=str)
    imode.add_argument('-p', '--postgres',      help='Read from PostgresqlDB.', type=str)
    omode = parser.add_mutually_exclusive_group(required=False)
    omode.add_argument('-c', '--csv',           help='Output data as CSV.',     action='store_true')
    omode.add_argument('-j', '--json',          help='Output data as JSON.',    action='store_true')
    parser.add_argument('-f', '--file',         help='Write data to file',      default=False)
    parser.add_argument('-b', '--begin',        help='Begin date (inclusive), format: yyyy-mm-dd', type=valid_date, default="2005-01-01")
    parser.add_argument('-u', '--until',        help='Until date (exclusive), format: yyyy-mm-dd', type=valid_date, default="2005-01-02")
    parser.add_argument('-t', '--type',         help='Type of data source (routeviews|riperis|?).', type=str, default="routeviews")
    parser.add_argument('-s', '--subtype',      help='Subtype of data source (route-view.wide|rrc01|?)', type=str, default="route-views.wide")
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

    oopts = ('csv', args['file'])
    if args['json']:
        oopts = ('json', args['file'])

    begin = args['begin']
    until = args['until']
    maptype = args['type']
    subtype = args['subtype']

    if args['postgres']:
        rres = retrieve_postgres(args['postgres'], begin, until, maptype, subtype)
        pres = process_data(rres)
        out = origin_ttl(pres)
        output(out, oopts)
    else:
        print_error('No valid data source found!')
    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))

if __name__ == "__main__":
    main()
