#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import re
import sys
import json
import psycopg2
import StringIO
import multiprocessing as mp
from bz2 import BZ2File
from datetime import datetime, timedelta
#from pyasn import mrtx
# own imports
import mrtx

verbose = False
warning = False
logging = False

re_file_rv = re.compile('rib.(\d+).(\d\d\d\d).bz2')
re_file_rr = re.compile('bview.(\d+).(\d\d\d\d).gz')

re_path_rv = re.compile('.*/([a-z0-9\.-]+)/bgpdata/\d\d\d\d.\d\d/RIBS.*')
re_path_rr = re.compile('.*/(rrc\d\d)/\d\d\d\d.\d\d.*')

existing_data = list()

prefix_ids = dict()
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

def parseOrigins(fin):
    print_log("call parseOrigins (%s)"  % (fin))
    f = (BZ2File(fin, 'rb'), gzip.open(fin, 'rb'))[fin.lower().endswith('.gz')]
    data = mrtx.parse_mrt_file(f, print_progress=verbose)
    f.close()
    pfxo = list()
    for prefix, origins in data.items():
        pfx = dict()
        pfx['prefix'] = prefix
        pfx['origins'] = list()
        for o in list(origins):
            if isinstance(o, set) or isinstance(o,list):
                for osub in list(o):
                    if str(osub) not in pfx['origins']:
                        pfx['origins'].append(str(osub))
            else:
                if str(o) not in pfx['origins']:
                    pfx['origins'].append(str(o))
        pfxo.append(pfx)
    return pfxo

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
    dt = "%s-%s-%s %s:%s" % (str(date[0:4]),str(date[4:6]),
                             str(date[6:8]),str(time[0:2]),str(time[2:4]))
    ts = int((datetime.strptime(dt, "%Y-%m-%d %H:%M") -
             datetime(1970, 1, 1)).total_seconds())
    return ts, maptype, subtype

def worker(fin):
    ts0, mt0, st0 = parseFilename(fin)
    origins = parseOrigins(fin)
    data = dict()
    data['timestamp'] = ts0
    data['maptype'] = mt0
    data['subtype'] = st0
    data['origins'] = origins
    return data

def workerThread(inq,outq):
    print_log("start workerThread")
    for fin in iter(inq.get, 'DONE'):
        try:
            data = worker(fin)
            outq.put(data)
        except Exception, e:
            print_error("%s failed with: %s" %
                        (mp.current_process().name, e.message))
    return True

def output(data, opts):
    if opts['output'] == 'json':
        outputJSON(data, opts['params'])
    elif opts['output'] == 'postgres':
        outputPostgres(data, opts['params'])
    elif opts['output']:
        print_info ("using %s with params %s." %
                    (opts['database'],opts['params']))
    else:
        outputStdout(data)

def outputJSON(data,fout):
    try:
        if not fout.lower().endswith('.gz'):
            fout = fout+".gz"
        with gzip.open(fout, "ab") as f:
            f.write(json.dumps(data)+'\n')
    except:
        print_error("Failed to write data as JSON to file %s." % (fout))

def outputPostgres(data,dbconnstr):
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("%s failed with: %s" % (mp.current_process().name, e.message))
        print_error("outputPG: connecting to database")
        sys.exit(1)

    cur = con.cursor()
    query_dataset = "SELECT id FROM t_datasets " \
                    "WHERE ts = %s AND  maptype = %s AND subtype = %s"
    insert_dataset = "INSERT INTO t_datasets (ts, maptype, subtype) " \
                     "VALUES (%s,%s,%s) RETURNING id"
    query_prefix = "SELECT id FROM t_prefixes WHERE prefix = %s"
    insert_prefix = "INSERT INTO t_prefixes (prefix) VALUES (%s) RETURNING id"
    insert_origin = "INSERT INTO t_origins VALUES (%s,%s,%s)"

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

    # create new dataset object in database, if not existing
    did = 0
    ts_str = datetime.fromtimestamp(
                data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    ts_year = ts_str.split('-')[0]
    ts_month =  ts_str.split('-')[1]
    t_origins_ym = "t_origins_"+ts_year+"_"+ts_month
    create_table_ym = ( "CREATE TABLE IF NOT EXISTS " +
                        t_origins_ym + " () " +
                        "INHERITS (t_origins)")
    try:
        cur.execute(create_table_ym)
        con.commit()
    except Exception, e:
        print_error("Error creating table (%s), failed with %s" %
                    (t_origins_ym, e.message))
        con.rollback()
    try:
        cur.execute(query_dataset, [ts_str,data['maptype'],data['subtype']])
        did = cur.fetchone()[0]
    except:
        # dataset does not exists, so create and fill it up
        con.rollback()
        cur.execute(insert_dataset, [ts_str,data['maptype'],data['subtype']])
        con.commit()
        did = cur.fetchone()[0]
    origins = data['origins']
    prefix_new = set()
    # find prefixes not in database
    for p in origins:
        pid = 0
        ptmp = p['prefix']
        if ptmp.endswith('/32'):
            ptmp = p['prefix'][:-3]
        if ptmp not in prefix_ids:
            prefix_new.add(p['prefix'])
    # write new prefixes to database
    if len(prefix_new) > 0:
        print_log("#new prefixes: %s" % (str(len(prefix_new))))
        pfx_str = '\n'.join(x for x in prefix_new)
        f_pfx = StringIO.StringIO(pfx_str)
        try:
            cur.copy_from(f_pfx, 't_prefixes', columns=('prefix'))
        except Exception, e:
            print_error("COPY TO t_prefixes failed with: %s" % (e.message))
            con.rollback()
    # update prefix dict
    try:
        cur.execute(query_all_prefixes)
        pfx = cur.fetchall()
        prefix_ids = dict((pfx[i][0], pfx[i][1]) for i in range(len(pfx)))
    except Exception, e:
        print_error("QUERY t_prefixes (2) failed with: %s" % (e.message))
        con.rollback()
    t_file = "/tmp/" + t_origins_ym + "." + data['maptype'] + "." + data['subtype'] + ".copy"
    # insert all origins into database
    f = open(t_file, "wb")
    for p in origins:
        pid = 0
        ptmp = p['prefix']
        if ptmp.endswith('/32'):
            ptmp = p['prefix'][:-3]
        if ptmp in prefix_ids:
            pid = prefix_ids[ptmp]
        else:
            try:
                cur.execute(query_prefix, [p['prefix']])
                pid = cur.fetchone()[0]
            except:
                con.rollback()
                cur.execute(insert_prefix, [p['prefix']])
                con.commit()
                pid = cur.fetchone()[0]
            prefix_ids[p['prefix']] = pid
        if pid > 0:
            for b in p['origins']:
                c = b.split()
                for a in c:
                    if (int(a)>0) and (int(did)>0):
                        line = str(did)+"\t"+str(pid)+"\t"+str(a)+"\n"
                        line = line.encode('utf-8').decode('utf-8','ignore').encode("utf-8")
                        f.write(line)
    f.close()
    try:
        copy_from_stdin = "COPY %s FROM STDIN"
        with open(t_file, "rb") as f:
            cur.copy_expert(sql=copy_from_stdin % t_origins_ym, file=f)
            con.commit()
        #cur.execute(copy_origins)
        #con.commit()
    except Exception, e:
        print_error("COPY t_origins FROM file failed with: %s" %
                    (e.message))
        con.rollback()

def outputStdout(data):
    print (json.dumps(data, sort_keys=True, indent=2, separators=(',', ': ')))

def outputThread(outq, opts):
    print_log("start outputThread")
    while True:
        odata = outq.get()
        if (odata == 'DONE'):
            break
        try:
            output(odata, opts)
        except Exception, e:
            print_error("%s failed with: %s" %
                        (mp.current_process().name, e.message))
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
    parser.add_argument('-t', '--threads',
                        help='Use threads for parallel and faster processing.',
                        action='store_true', default=False)
    parser.add_argument('-n', '--numthreads',
                        help='Set number of threads.',
                        type=int, default=None)
    imode = parser.add_mutually_exclusive_group(required=True)
    imode.add_argument('-s', '--single',
                        help='Process a single file.')
    imode.add_argument('-b', '--bulk',
                        help='Bulk process directory (optional recursive).')
    parser.add_argument('-r', '--recursive',
                        help='Search directories recursivly if in bulk mode.',
                        action='store_true')
    omode = parser.add_mutually_exclusive_group(required=False)
    omode.add_argument('-j', '--json',
                        help='Write data to JSON file.',
                        default=False)
    omode.add_argument('-p', '--postgres',
                        help='Write data to PostgresqlDB.',
                        default=False)
    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']

    global warning
    warning   = args['warning']

    global logging
    logging   = args['logging']

    recursive = args['recursive']
    threads   = args['threads']
    workers   = args['numthreads']
    if not workers:
        workers = mp.cpu_count() / 2

    bulk      = args['bulk']
    single    = args['single']

    oopts = dict()
    oopts['output'] = False
    if args['postgres']:
        oopts['output'] = 'postgres'
        oopts['params'] = args['postgres']
    if args['json']:
        oopts['output'] = 'json'
        oopts['params'] = args['json']

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
                for filename in [f for f in filenames
                        if (re_file_rv.match(f) or re_file_rr.match(f))]:
                    all_files.append(os.path.join(dirpath, filename))
        else:
            for filename in [f for f in os.listdir(bulk)
                        if (re_file_rv.match(f) or re_file_rr.match(f))]:
                all_files.append(os.path.join(bulk, filename))

        all_files.sort()
        print_log("matching files: %d" % (len(all_files)))

        if threads:
            mgr = mp.Manager()
            input_queue = mgr.Queue()
            output_queue = mgr.Queue()
            processes = []
            # fill input queue
            for f in all_files:
                input_queue.put(f)
            # start workers to calc stats
            for w in xrange(workers):
                p = mp.Process(target=workerThread,
                            args=(input_queue,output_queue))
                p.start()
                processes.append(p)
                input_queue.put('DONE')
            # start output process to
            output_p = mp.Process(target=outputThread,
                               args=(output_queue,oopts))
            output_p.start()

            for p in processes:
                p.join()

            output_queue.put('DONE')
            output_p.join()
        else:
            for f in all_files:
                odata = worker(f)
                output(odata, oopts)
    elif single:
        print_log("mode: single")
        if os.path.isfile(single):
            odata = worker(single)
            output(odata, oopts)
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
