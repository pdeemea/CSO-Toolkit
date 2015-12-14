#!/usr/bin/python
#
# Copyright (C) Pivotal Inc 2014. All Rights Reserved. 
# Alexey Grishchenko (agrishchenko@gopivotal.com)
#
# This script can be used to perform multi-thread restore through
# the master server in case of the segment configuration change.
# You can specify number of threads to restore, recommended values
# are from 4 to 6
#
# Instrunctions:
#
# Script performs serial restore of the backup files in case
# of the cluster topology change.
# Usage:
# ./serial_restore.py -n thread_number -t backup_timestamp -b backup_directory -d dbname [-p gpadmin_password]
# Parameters:
#     thread_number    - number of parallel threads to run
#     backup_timestamp - timestamp of the backup to be restored
#     backup_directory - folder with the complete backup set visible from the current server
#     dbname           - name of the database to restore to
#     gpadmin_password - password of the gpadmin user
#
import os, sys, re, os.path, subprocess, csv, time

try:
    from optparse import Option, OptionParser 
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from gppylib.gpcoverage import GpCoverage
    from gppylib import userinput
except ImportError, e:    
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')    
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-t', '--timestamp', type='string')
    parser.add_option('-b', '--backupdir', type='string')
    parser.add_option('-d', '--dbname',    type='string')
    parser.add_option('-p', '--password',  type='string')
    parser.add_option('-n', '--nthreads',  type='int')
    (options, args) = parser.parse_args()
    if options.help or (not options.dbname and not options.filename):
        print """Script performs serial restore of the backup files in case
of the cluster topology change.
Usage:
./serial_restore.py -n thread_number -t backup_timestamp -b backup_directory -d dbname [-p gpadmin_password]
Parameters:
    thread_number    - number of parallel threads to run
    backup_timestamp - timestamp of the backup to be restored
    backup_directory - folder with the complete backup set visible from the current server
    dbname           - name of the database to restore to
    gpadmin_password - password of the gpadmin user"""
        sys.exit(0)
    if not options.timestamp:
        logger.error('Failed to start utility. Please, specify backup timestamp with "-t" key')
        sys.exit(1)
    if not options.backupdir:
        logger.error('Failed to start utility. Please, specify backup directory with "-b" key')
        sys.exit(1)
    if not options.dbname:
        logger.error('Failed to start utility. Please, specify database name with "-d" key')
        sys.exit(1)
    if not options.nthreads:
        options.nthreads = 1
    return options
    
def execute(dburl, query):
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
        return rows
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)

def run_sync(command):
    try:
        pid = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        pid.wait()
        retcode = pid.returncode
        if retcode <> 0:
            logger.error("Error during execution of the command '%'" % command)
            sys.exit(3)
    except:
        logger.error('Exception ' + str(sys.exc_info()[1]))
        sys.exit(3)

def restore_segments(backup_files, threads):
    restore_command = "cat %s/%s | /bin/gunzip -c | psql -d %s"
    running = []
    isStopping = 0
    while len(backup_files) > 0 or len(running) > 0:
        for pid in running:
            if not pid.poll() is None:
                if pid.returncode == 0:
                    running.remove(pid)
                else:
                    pidret = pid.communicate()
                    logger.error ('Restore failed for one of the segments')
                    logger.error (redret[0])
                    logger.error (redret[1])
                    isStopping = 1
        if isStopping == 0 and len(running) < threads and len(backup_files) > 0:
            backup_file = backup_files.pop()
            logger.info ('    Restoring %s' % backup_file)
            pid_restore_command = restore_command % (options.backupdir, backup_file, options.dbname)
            pid = subprocess.Popen(pid_restore_command, shell=True, stdout=subprocess.PIPE)
            running.append(pid)
        if isStopping == 1 and len(running) == 0:
            break
        time.sleep(10)
        
def orchestrator(options):
    # Check that the backup set is complete
    if not os.path.exists (options.backupdir):
        logger.error('Backup directory you specified does not exist: ' + options.backupdir)
        sys.exit(2)
    if not os.path.isdir (options.backupdir):
        logger.error('Backup folder you specified is not a directory: ' + options.backupdir)
        sys.exit(2)
    logger.info ('=== Master backup files found:')
    backupset = dict()
    for file in os.listdir(options.backupdir):
        if os.path.isfile (os.path.join(options.backupdir, file)):
            if file[:11]=='gp_dump_1_1' and options.timestamp in file and '.gz' in file:
                logger.info ('    ' + file)
                if 'post_data' in file:
                    backupset['post'] = file
                else:
                    backupset['master'] = file
    logger.info ('=== Segment backup files found:')
    backupset['segment'] = []
    for file in os.listdir(options.backupdir):
        if os.path.isfile (os.path.join(options.backupdir, file)):
            if file[:9]=='gp_dump_0' and options.timestamp in file and '.gz' in file:
                logger.info ('    ' + file)
                backupset['segment'].append(file)
    if not 'post' in backupset:
        logger.error ('Cannot find backup post_data file. Stopping')
        sys.exit(4)
    if not 'master' in backupset:
        logger.error ('Cannot find backup master data file. Stopping')
        sys.exit(4)
    if len(backupset['segment']) == 0:
        logger.error ('Cannot find backup segment files. Stopping')
        sys.exit(4)
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = 'template1',
                         username = 'gpadmin',
                         password = options.password)
    query = "select 1 from pg_database where datname = '%s'" % options.dbname
    if execute (dburl, query) != [[1]]:
        logger.error ("Database '%s' does not exist. Create it before running this script" % options.dbname)
        sys.exit(4)
    if not userinput.ask_yesno(None, "Confirm that database %s is empty and ready for restore?" % options.dbname, 'N'):
        logger.error ("Restore terminated by user request")
        sys.exit(6)
    if not userinput.ask_yesno(None, "Do you want to continue with this resore?", 'N'):
        logger.error ("Restore terminated by user request")
        sys.exit(6)
    logger.info ('=== Restoring master server backup file %s' % backupset['master'])
    run_sync("cat %s/%s | /bin/gunzip -c | psql -d %s" % (options.backupdir, backupset['master'], options.dbname))
    logger.info ('=== Restoring Segment Servers in %d threads' % options.nthreads)
    restore_segments(backupset['segment'], options.nthreads)
    logger.info ('=== Restoring master server post data file %s' % backupset['master'])
    run_sync("cat %s/%s | /bin/gunzip -c | psql -d %s" % (options.backupdir, backupset['post'], options.dbname))
    logger.info ('Restore complete')
    
#------------------------------- Mainline --------------------------------

#Initialization
coverage = GpCoverage()
coverage.start()
logger = get_default_logger()

#Parse input parameters and check for validity
options = parseargs()

#Print the partition list
orchestrator(options)

#Stopping
coverage.stop()
coverage.generate_report()
        