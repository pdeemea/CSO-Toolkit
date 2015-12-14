import datetime as dt
import sys
import random
from multiprocessing import Process
import subprocess
import os

def execute_for_timing(fout, ferr, thread_id, filename, database):
    try:
        n1 = dt.datetime.now()
        s = subprocess.Popen ('psql -d %s -f /home/gpadmin/audit_201411/sql/%s' % (database, filename),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              shell=True)
        stdout, stderr = s.communicate()
        fout.write(stdout)
        ferr.write(stderr)
        n2 = dt.datetime.now()
        return ((n2-n1).seconds*1e6 + (n2-n1).microseconds) / 1e6
    except Exception, ex:
        ferr.write('Failed to execute the statement "%s" on the database "%s". Please, check log file for errors.\n' % (filename, database))
        ferr.write(str(ex) + '\n')
        return 0.0

def run_benchmark(thread_id, seconds, proc_files, database):
    random.seed()
    n1 = dt.datetime.now()
    fout = open('/home/gpadmin/audit_201411/data/thread_%d.stdout' % thread_id, 'w')
    ferr = open('/home/gpadmin/audit_201411/data/thread_%d.stderr' % thread_id, 'w')
    while (dt.datetime.now() - n1).seconds < seconds:
        ind = random.randint(0, len(proc_files)-1)
        t = execute_for_timing(fout, ferr, thread_id, proc_files[ind], database)
        print '%d|%s|%f' % (thread_id, proc_files[ind], t)
    fout.close()
    ferr.close()
    return
        
def main():
    n_threads = int(sys.argv[1])
    n_seconds = int(sys.argv[2])
    database  = sys.argv[3]
    proc_files = [f for f in os.listdir('/home/gpadmin/audit_201411/sql')]
    print proc_files
    proc_list  = []
    for i in range(n_threads):
        p = Process(target=run_benchmark, args=(i,n_seconds,proc_files,database,))
        p.start()
        proc_list.append(p)
    for p in proc_list:
        p.join()

main()

