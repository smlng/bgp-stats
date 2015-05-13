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
from multiprocessing import Process, Queue, cpu_count, current_process

verbose = False
warning = False
logging = False

re_file_rv = re.compile('rib.(\d+).(\d\d\d\d).bz2')
re_file_rr = re.compile('bview.(\d+).(\d\d\d\d).gz')

re_path_rv = re.compile('.*/([a-z0-9\.-]+)/bgpdata/\d\d\d\d.\d\d/RIBS.*')
re_path_rr = re.compile('.*/(rrc\d\d)/\d\d\d\d.\d\d.*')

existing_data = list()

prefix_ids = dict()
t_file = "/tmp/t_origins.copy"
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging',      help='Ouptut logging.', action='store_true')
    parser.add_argument('-w', '--warning',      help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',      help='Verbose output with debug info, logging, and warnings.', action='store_true')
    parser.add_argument('-t', '--threads',      help='Use threads for parallel and faster processing.', action='store_true', default=False)
    parser.add_argument('-n', '--numthreads',   help='Set number of threads.', type=int, default=None)
    imode = parser.add_mutually_exclusive_group(required=True)
    imode.add_argument('-s', '--single',        help='Process a single file, results are printed to STDOUT.')
    imode.add_argument('-b', '--bulk',          help='Process a bunch of files in given directory (optional recursive).')
    parser.add_argument('-r', '--recursive',    help='Search directories recursivly if in bulk mode.', action='store_true')
    omode = parser.add_mutually_exclusive_group(required=False)
    omode.add_argument('-j', '--json',          help='Write data to JSON file.',    default=False)
    omode.add_argument('-c', '--couchdb',       help='Write data to CouchDB.',      default=False)
    omode.add_argument('-m', '--mongodb',       help='Write data to MongoDB.',      default=False)
    omode.add_argument('-p', '--postgres',      help='Write data to PostgresqlDB.', default=False)
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

    start_time = datetime.now()
    print_log("START: " + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    
    end_time = datetime.now()
    print_log("FINISH: " + end_time.strftime('%Y-%m-%d %H:%M:%S'))
    done_time = end_time - start_time
    print_log("  processing time [s]: " + str(done_time.total_seconds()))

if __name__ == "__main__":
    main()
