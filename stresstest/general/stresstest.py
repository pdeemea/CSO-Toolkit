import datetime as dt
import time
import sys
import random
from multiprocessing import Process
import subprocess
import os
import json

def execute_for_timing(database, username, query=None, sqlfile=None):
    reserr = ''
    resval = 0.0
    if query is not None:
        command = query
    else:
        command = sqlfile
    try:
        n1 = dt.datetime.now()
        if query is not None:
            s = subprocess.Popen (""" psql -d %s -U %s -c "%s" """ % (database, username, query),
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  shell=True)
        else:
            DEVNULL = open(os.devnull, 'wb')
            s = subprocess.Popen (""" psql -d %s -U %s -f %s """ % (database, username, sqlfile),
                                  stdout=DEVNULL,
                                  stderr=subprocess.PIPE,
                                  shell=True)
        stdout, stderr = s.communicate()
        n2 = dt.datetime.now()
        resval = ((n2-n1).seconds*1e6 + (n2-n1).microseconds) / 1e6
        if not (stderr is None) and stderr.strip() <> '':
            reserr += 'Failed to execute "%s" on the database "%s"\n' % (command, database)
            reserr += stderr
    except Exception, ex:
        reserr += 'Failed to execute "%s" on the database "%s"\n' % (command, database)
        reserr += '%s\n' % str(ex)
    return resval, reserr

def write_benchmark (thread_id, test_name, seconds, database, username, log_dir, sql_files):
    random.seed()
    fout = open('%s/%s_write_%d.csv' % (log_dir,test_name,thread_id), 'w')
    ferr = open('%s/%s_write_%d.err' % (log_dir,test_name,thread_id), 'w')
    n1 = dt.datetime.now()
    while (dt.datetime.now() - n1).seconds < seconds:
        ind = random.randint(0, len(sql_files)-1)
        now = dt.datetime.now()
        t, err = execute_for_timing(database, username, sqlfile=sql_files[ind])
        if not (err is None) and err <> '':
            ferr.write(err)
        else:
            fout.write('%s|%s|%d|%f\n' % (now.strftime('%Y-%m-%d %H:%M:%S'),sql_files[ind],thread_id, t))
    fout.close()
    ferr.close()
    return

def read_benchmark(thread_id, test_name, seconds, database, username, log_dir, queries, qtype):
    random.seed()
    fout = open('%s/%s_read_%d_%s.csv' % (log_dir,test_name,thread_id,qtype), 'w')
    ferr = open('%s/%s_read_%d_%s.err' % (log_dir,test_name,thread_id,qtype), 'w')
    n1 = dt.datetime.now()
    qids = queries.keys()
    while (dt.datetime.now() - n1).seconds < seconds:
        ind = random.randint(0, len(qids)-1)
        now = dt.datetime.now()
        t, err = execute_for_timing(database, username, query=queries[qids[ind]])
        if not (err is None) and err <> '':
            ferr.write(err)
        else:
            fout.write('%s|%s|%d|%s|%f\n' % (qtype, now.strftime('%Y-%m-%d %H:%M:%S'),thread_id, qids[ind], t))
    fout.close()
    ferr.close()
    return

def readers_main(config, test_number, username, database, log_dir):
    def read_queries(filename):
        f = open(filename, 'r')
        queries = dict()
        queries['mixed'] = dict()
        for line in f:
            type, id, query = line.split('!')
            if not type in queries:
                queries[type] = dict()
            queries[type][id] = query
            queries['mixed'][type + id] = query
        return queries
    n_threads = None
    test_name = config['tests'][test_number]['test_name']
    if 'readers' in config['tests'][test_number]:
        if 'test_threads_number' in config['tests'][test_number]['readers']:
            n_threads = config['tests'][test_number]['readers']['test_threads_number']
    if not n_threads is None and n_threads > 0:
        print 'readers: n_threads = %d' % n_threads
        n_seconds = config['tests'][test_number]['test_runtime_seconds']
        n_tests   = len(config['tests'][test_number]['readers'].get('tests_to_run'))
        if n_tests is None or n_tests == 0:
            print 'readers: WARNING: Number of tests specified by "tests_to_run" is zero, readers test is omitted'
        else:
            n_seconds = int(n_seconds/n_tests)
            print 'readers: running %d test for %d seconds each' % (n_tests, n_seconds)
            tests_to_run = config['tests'][test_number]['readers']['tests_to_run']
            badtests = set(tests_to_run) - set(["cpu", "memory", "disk", "network", "mixed"])
            if len(badtests) > 0:
                for test in badtests:
                    print 'readers: ERROR: Specified test "%s" is not in the list of available tests and cannot be run'
            else:
                print 'readers: running following tests: %s' % ', '.join(tests_to_run)
                stresstest_sqls_file = config['tests'][test_number]['readers']['stresstest_sqls_file']
                queries = dict()
                try:
                    queries   = read_queries(stresstest_sqls_file)
                except Exception, ex:
                    print 'readers: ERROR: problem while reading the file %s' % stresstest_sqls_file
                    print 'readers:   --> %s' % str(ex)
                if len(queries) > 0:
                    badtests = set(tests_to_run) - set(queries.keys())
                    if len(badtests) > 0:
                        print 'readers: ERROR: Specified tests "%s" is not in the file with test queries "%s"' % (','.join(badtests), stresstest_sqls_file)
                    else:
                        for t in tests_to_run:
                            q = len(queries[t])
                            print 'readers:     test "%s" has %d queries specified' % (t, q)
                        print 'readers: ready to start'
                        for test in tests_to_run:
                            print 'readers: STARTING THREADS FOR THE TEST "%s" TYPE "%s"' % (test_name, test)
                            proc_list = []
                            for i in range(n_threads):
                                p = Process(target=read_benchmark, args=(i, test_name, n_seconds, database, username, log_dir, queries[test], test))
                                p.start()
                                proc_list.append(p)
                            for p in proc_list:
                                p.join()
    else:
        print 'readers: no configuration defined for readers'
    return

def writers_main(config, test_number, username, database, log_dir):
    n_threads = None
    test_name = config['tests'][test_number]['test_name']
    if 'writers' in config['tests'][test_number]:
        if 'test_threads_number' in config['tests'][test_number]['writers']:
            n_threads = config['tests'][test_number]['writers']['test_threads_number']
    if not n_threads is None and n_threads > 0:
        print 'writers: n_threads = %d' % n_threads
        n_seconds = config['tests'][test_number]['test_runtime_seconds']
        print 'writers: n_seconds = %d' % n_seconds
        sql_files_directory = config['tests'][test_number]['writers']['sql_files_directory']
        sql_files = [sql_files_directory+'/'+f for f in os.listdir(sql_files_directory) if os.path.isfile(sql_files_directory+'/'+f)]
        print 'writers: input files -->'
        for f in sql_files:
            print 'writers:     %s' % f
        if len(sql_files) == 0:
            print 'writers: WARNING: No SQL files found in the directory %s, writers test is omitted' % sql_files_directory
        else:
            print 'writers: ready to start'
            print 'writers: STARTING THREADS FOR THE TEST "%s"' % test_name
            proc_list = []
            for i in range(n_threads):
                p = Process(target=write_benchmark, args=(i, test_name, n_seconds, database, username, log_dir, sql_files))
                p.start()
                proc_list.append(p)
            for p in proc_list:
                p.join()
    else:
        print 'writers: no configuration defined for writers'
    return
    
# Main function invokes separate threads for readers and writers test
def main():
    config    = json.load(open(sys.argv[1], 'r'))
    testnum   = len(config['tests'])
    print '========== READ METADATA FOR %d TESTS ==========' % testnum
    username      = config['username']
    username_read = config.get('username_read')
    if username_read is None or username_read == '':
        username_read = username
    database = config['database']
    log_dir  = config['logs_directory']
    print '---- USING DATABASE "%s" UNDER USER "%s" ----' % (database, username)
    print '---- LOGGING TO "%s"' % log_dir        
    for i in range(testnum):
        print '========== STARTING THE TEST "%s" ==========' % config['tests'][i]['test_name']
        readers   = Process(target=readers_main, args=(config,i,username_read,database,log_dir))
        writers   = Process(target=writers_main, args=(config,i,username,     database,log_dir))
        readers.start()
        time.sleep(2)
        writers.start()
        writers.join()
        readers.join()
    return
    
main()

# python stresstest.py 5 60 dssprod data/stresstest_queries.sql
# nohup python stresstest.py 20 7200 dssprod /home/gpadmin/audit_201411/data/stresstest_queries_big.sql >/home/gpadmin/audit_201411/data/stresstest.out 2>&1 &