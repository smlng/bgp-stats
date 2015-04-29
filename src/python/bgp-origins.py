#!/usr/bin/python

from __future__ import print_function

import argparse
import gzip
import os
import re
import sys
import json

from bz2 import BZ2File
from datetime import datetime, timedelta
from multiprocessing import Process, Queue, cpu_count

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
    pfxo = dict()
    for prefix, origins in data.items():
        if prefix not in pfxo:
            pfxo[prefix] = list()
        for o in list(origins):
            if str(o) not in pfxo[prefix]:
                pfxo[prefix].append(str(o))
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
    dt = "%s-%s-%s %s:%s" % (str(date[0:4]),str(date[4:6]),str(date[6:8]),str(time[0:2]),str(time[2:4]))
    ts = int((datetime.strptime(dt, "%Y-%m-%d %H:%M") - datetime(1970, 1, 1)).total_seconds())
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
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return true

def output(data, opts):
    if opts['database']:
        print_info ("using %s with params %s." % (opts['database'],opts['params']))
    if opts['json']:
        outputJSON(data, opts['json'])
    if not opts['database'] and not opts['json']:
        outputStdout(data)

def outputJSON(data,fout):
    try:
        if not fout.lower().endswith('.gz'):
            fout = fout+".gz"
        with gzip.open(fout, "ab") as f:
            f.write(json.dumps(data)+'\n')
    except:
        print_error("Failed to write data as JSON to file %s." % (fout))

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
            print_error("%s failed on %s with: %s" % (current_process().name, url, e.message))
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    parser.add_argument('-t', '--threads',      help='Use threads for parallel and faster processing.', action='store_true', default=False)
    parser.add_argument('-n', '--numthreads',   help='Set number of threads.', type=int, default=None)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('-s', '--single',         help='Process a single file, results are printed to STDOUT.')
    mode.add_argument('-b', '--bulk',           help='Process a bunch of files in given directory (optional recursive).')
    parser.add_argument('-r', '--recursive',    help='Search directories recursivly if in bulk mode.', action='store_true')   
    parser.add_argument('-j', '--json',         help='Write data to file (not STDOUT), even if database is enabled.', default=False)
    db = parser.add_mutually_exclusive_group(required=False)
    db.add_argument('-c', '--couchdb',          help='Write data to CouchDB.', default=False)
    db.add_argument('-m', '--mongodb',          help='Write data to MongoDB.', default=False)
    db.add_argument('-p', '--postgres',         help='Write data to PostgresqlDB.', default=False)
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
        workers = cpu_count() / 2

    bulk      = args['bulk']
    single    = args['single']

    oopts = dict()
    oopts['json'] = args['json']
    oopts['database'] = False
    if args['couchdb']:
        oopts['database'] = 'couchdb'
        oopts['params'] = args['couchdb']
    if args['mongodb']:
        oopts['database'] = 'mongodb'
        oopts['params'] = args['mongodb']
    if args['postgres']:
        oopts['database'] = 'postgres'
        oopts['params'] = args['postgres']

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