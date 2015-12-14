import sys
import datetime as dt
import random
from multiprocessing import Process
import signal

try:
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))
logger = get_default_logger()

WORKLOAD_QUERY = "select count(*) from test where randtext ~ '.*(aaa)|(bbb)|(ccc).*' or randtext ~ '.*[abc]{5}.*' or randtext ~ '[01234]{5,9}' or randtext ~ '[xyz]{10}'"

def raise_err (message):
    logger.error (message)
    sys.exit(3)

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help',  action='store_true')
    parser.add_option('-f', '--fromthreads', type='int')
    parser.add_option('-t', '--tothreads',   type='int')
    parser.add_option('-d', '--database',    type='string')
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script generates a big CPU workload on the cluster within configured diapason of
thread number to check the elasicity of cluster CPU resources
Usage:
python cpu_stresstest.py -f fromthreads -t tothreads -d database
    -t | --fromthreads - Lower bound of thread number to start
    -t | --tothreads   - Upper bound of thread number to start
    -d | --database    - Database to run the test on
"""
        sys.exit(0)
    if not options.fromthreads:
        raise_err('You must specify the lower bound of thread number with -f parameter')
    if not options.tothreads:
        raise_err('You must specify the upper bound of thread number with -t parameter')
    if not options.database:
        raise_err('You must specify the database name with -d parameter')
    return options

def execute(dburl, query):
    rows = [[]]
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, 'set enforce_virtual_segment_number = 16')
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
    return rows

def generate_load(database):
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = database)
    n1 = dt.datetime.now()
    execute(dburl, WORKLOAD_QUERY)
    n2 = dt.datetime.now()
    runtime = (n2-n1).seconds + ((n2-n1).microseconds / 1000000.)
    print runtime
    return

def run_workload(n_threads, database):
    proc_list = []
    for i in range(n_threads):
        p = Process(target=generate_load, args=(database,))
        p.start()
        proc_list.append(p)
    for p in proc_list:
        p.join()

def main():
    options = parseargs()
    fromthreads = int(options.fromthreads)
    tothreads   = int(options.tothreads)
    for threads in range(fromthreads, tothreads+1):
        n1 = dt.datetime.now()
        run_workload(threads, options.database)
        n2 = dt.datetime.now()
        runtime = (n2-n1).seconds + ((n2-n1).microseconds / 1000000.)
        print 'Total: %d|%f' % (threads,runtime)

main()
