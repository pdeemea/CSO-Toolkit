#!/usr/bin/env python

import os
import sys

try:
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib import gplog
    from gppylib.commands.unix import getLocalHostname, getUserName
    from gppylib.db import dbconn
    from gppylib.db.catalog import doesSchemaExist, getDatabaseList
    from pygresql.pg import DatabaseError
    from gppylib import userinput
    from gppylib.commands.base import Command
except ImportError, e:
    sys.exit('ERROR: Cannot import Greenplum modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

GPTEXT_SCHEMA   = 'gptext'
EXPUNGE_COMMAND = """nohup curl http://%s:%d/solr/%s/update -H 'Content-Type: text/xml' --data-binary '<commit expungeDeletes="true"/>' >/dev/null 2>/dev/null &"""

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-i', '--index',      type='string')
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script is calling Solr commit with the expungeDeletes flag to cause
all the index segments with deletes in them to clean up their segments
from the deleted records
Usage:
python gptext_expunge_deletes.py -i index_name
    -i | --index - Name of the index to expunge
Examples:
    python gptext_expunge_deletes.py -i test.public.test_table
"""
        sys.exit(0)
    if not options.index:
        logger.error('You must specify index name with -i or --index key')
        sys.exit(3)
    return options

def gptext_schema_exists(db):
    exists = False
    try:
        url = dbconn.DbURL(dbname=db)
        conn = dbconn.connect(url)
        exists = doesSchemaExist(conn, GPTEXT_SCHEMA)
        conn.close()
    except:
        pass
    return exists

def find_gptext_schema():
    logger.info('Locating %s schema...' % GPTEXT_SCHEMA)
    gptext_schema_db = None
    url = dbconn.DbURL()
    conn = dbconn.connect(url)
    databases = getDatabaseList(conn)
    conn.close()
    for db in databases:
        if gptext_schema_exists(db[0]):
            gptext_schema_db = db[0]
            break
    return gptext_schema_db

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

def validate_solr_up(dburl):
    logger.info('Validating GPText is up with no failed segments...')
    gptext_seg_down = execute(dburl, "SELECT count(*) FROM gptext.status() WHERE status <> 'u';")
    if gptext_seg_down <> [[0]]:
        logger.error('Cannot modify the index as %d Solr instances is down' % (gptext_seg_down[0][0]))
        sys.exit(3)
    return

def validate_index_exists(dburl, index):
    logger.info('Validating GPText index %s exists...')
    gptext_index_stats = execute(dburl, "SELECT count(*) FROM gptext.index_statistics('%s');" % index)
    if gptext_index_stats == [[0]]:
        logger.error('Index %s does not exist in GPText' % index)
        sys.exit(3)
    return

def get_solr_instances(dburl):
    logger.info('Getting GPText host configuration...')
    hosts = execute(dburl, "SELECT host, port FROM gptext.status() where role='p';")
    logger.info('Current Solr Primary Instances:')
    conf = dict()
    for h in hosts:
        hostname, port = h[0], h[1]
        if not hostname in conf:
            conf[hostname] = []
        conf[hostname].append(port)
    hostnames = sorted(conf.keys())
    for h in hostnames:
        logger.info('    %s : (%s)' % (h, ', '.join([str(x) for x in sorted(conf[h])]) ))
    return hosts

def gptext_index_expunge(index, hosts):
    logger.info('Starting expunge commands...')
    for host in hosts:
        hostname, port = host[0], host[1]
        cmd = Command("Expunging index %s on host %s port %d" % (index, hostname, port),
                      EXPUNGE_COMMAND % (hostname, port, index))
        cmd.run(validateAfter=True)
    return

def main():
    options = parseargs()
    database = find_gptext_schema()
    if not database:
        logger.error('Could not find schema %s.' % GPTEXT_SCHEMA)
        logger.error('Use the --database option to specify the database that')
        logger.error('contains the %s schema.' % GPTEXT_SCHEMA)
        sys.exit(1)
    url = dbconn.DbURL(dbname=database)
    validate_solr_up(url)
    validate_index_exists(url, options.index)
    hosts   = get_solr_instances(url)
    if not userinput.ask_yesno(None, "\nContinue with GPText index '%s' expunge?" % options.index, 'N'):
        logger.error ('User asked for termination')
        sys.exit(1)
    gptext_index_expunge(options.index, hosts)
    logger.info('Done')
    logger.info('To check the process use "ps -ef | grep curl" - it will show you all the expunge requests issued')

gplog.setup_tool_logging('gptext_expunge', getLocalHostname(), getUserName())
logger  = gplog.get_default_logger()
main()
