#!/usr/bin/env python

import os
import sys
import urllib
import xml.etree.ElementTree as ET

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
    
GPTEXT_SCHEMA = 'gptext'
STATUS_URL    = "http://%s:%d/solr/admin/cores?action=STATUS&core=%s"
    
def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-i', '--index',      type='string')
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script that prints detailed index information including amount of deleted
documents on each of the GPDB instances and index skew coefficients
Usage:
python gptext_index_details.py -i index_name
    -i | --index - Name of the index to analyze
Examples:
    python gptext_index_details.py -i test.public.test_table
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

def gptext_index_stats(index, hosts):
    logger.info('=================================================================================')
    logger.info('Statistics for index %s:' % index)
    total_documents = 0
    total_deleted   = 0
    size_bytes      = 0
    conf = dict()
    for h in hosts:
        hostname, port = h[0], h[1]
        if not hostname in conf:
            conf[hostname] = []
        conf[hostname].append(port)
    hostnames = sorted(conf.keys())
    stats = []
    for hostname in hostnames:
        logger.info('    Host %s:' % hostname)
        ports = sorted(conf[hostname])
        for port in ports:
            docs, deldocs, size = 0, 0, 0
            res = urllib.urlopen(STATUS_URL % (hostname, port, index)).read()
            root = ET.fromstring(res)
            for node in root.findall('.//*'):
                name = node.attrib.get('name')
                if name == 'numDocs':
                    docs = int(node.text)
                if name == 'deletedDocs':
                    deldocs = int(node.text)
                if name == 'sizeInBytes':
                    size = int(node.text)
            logger.info('        Port %d: Documents=%12d Deleted=%10d Size=%14d' % (port, docs, deldocs, size))
            stats.append([docs, deldocs, size])
    logger.info('=================================================================================')
    docs = [x[0] for x in stats]
    deldocs = [x[1] for x in stats]
    size = [x[2] for x in stats]
    if max(docs) > 0:
        logger.info('Index skew based on documents number: %f' %
                        (float(max(docs) - min(docs)) / float(sum(docs)) * float(len(docs))) )
        logger.info('Index skew based on index size: %f' %
                        (float(max(size) - min(size)) / float(sum(size)) * float(len(size))) )
        logger.info('Percent of deleted documents: %4.2f%%' %
                        (100. * float(sum(deldocs)) / float(sum(docs))) )
        logger.info('Total index size: %10.3f GB' %
                        (sum(size) / 1000. / 1000. / 1000.) )
        logger.info('Estimated index size after expunge: %10.3f GB' %
                        (sum(size) / 1000. / 1000. / 1000. * (1.0 - float(sum(deldocs)) / float(sum(docs)))) )
    else:
        logger.info('Index is empty')
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
    gptext_index_stats(options.index, hosts)
    logger.info('Done')
        
gplog.setup_tool_logging('gptext_details', getLocalHostname(), getUserName())
logger  = gplog.get_default_logger()
main()