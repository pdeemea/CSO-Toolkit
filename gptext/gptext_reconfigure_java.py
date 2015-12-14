import subprocess
import time
import datetime
try:
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.db import dbconn
    from gppylib.gplog import *
    from gppylib import userinput
    from pygresql.pg import DatabaseError
    from gppylib.commands.base import Command, REMOTE, ExecutionError
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))
logger = get_default_logger()

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

def getHostList(dburl):
    query = """
            select  hostname
                from gp_segment_configuration
                where preferred_role = 'p' and content <> -1
                group by hostname
        """
    res = execute(dburl, query)
    return [x[0] for x in res]

def getAmountOfHostsPerSegment(dburl):
    query = """
        select max(c)
            from (
                select  hostname,
                        count(*) as c
                    from gp_segment_configuration
                    where preferred_role = 'p' and content <> -1
                    group by hostname
                ) as q
        """
    res = execute(dburl, query)
    return int(res[0][0])

def getMasterRam():
    cmd = Command("Getting amount of free memory",
                  "cat /proc/meminfo | grep MemTotal")
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    if res.rc <> 0:
        logger.error('Failed to execute the statement "cat /proc/meminfo" on the local host')
        sys.exit(2)
    else:
        r = int([x for x in res.stdout.split(' ') if x][1])/1024
    return r

def getRAMSize(hosts):
    ramsize = None
    for host in hosts:
        cmd = Command("Getting amount of free memory",
                      "cat /proc/meminfo | grep MemTotal",
                      ctxt=REMOTE,
                      remoteHost=host)
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to execute the statement "cat /proc/meminfo" on the remote host %s' % host)
            sys.exit(2)
        else:
            r = int([x for x in res.stdout.split(' ') if x][1])/1024
            logger.info ('    %s - %d MB' % (host, r))
            if ramsize is None:
                ramsize = r
            else:
                if ramsize <> r:
                    logger.error ('Segment configuration is not symmetric. All the segments should have equal amount of RAM')
                ramsize = min(ramsize, r)
    return ramsize

def editGPTextConfig(java_opts):
    fi = open('/tmp/jetty.conf', 'r')
    fo = open('/tmp/jetty.conf_new', 'w')
    for line in fi:
        if not 'JAVA_OPTS' in line:
            fo.write(line)
        else:
            fo.write('export JAVA_OPTS="%s"\n' % java_opts)
    fi.close()
    fo.close()
    return

def tuneGPText (dburl, java_opts):
    logger.info ('Tuning GPText:')
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    query = """
            SELECT  gpsc.hostname as host,
                    pgfse.fselocation as datadir
                FROM pg_tablespace            as pgts,
                     pg_filespace             as pgfs,
                     pg_filespace_entry       as pgfse,
                     gp_segment_configuration as gpsc
            WHERE    pgts.spcfsoid = pgfse.fsefsoid
                AND pgfse.fsefsoid = pgfs.oid
                AND  pgfse.fsedbid = gpsc.dbid
                AND   pgts.spcname = 'pg_default'
                AND    pgfs.fsname = 'pg_system'
                AND   gpsc.content <> -1
            ORDER BY host, datadir
        """
    hostdirs = execute(dburl, query)
    for hdir in hostdirs:
        host, dir = hdir[0], hdir[1]
        logger.info ('    host: %s  dir: %s' % (host, dir))
        cmd = Command("Backup GPText config",
                      "cp %s/solr/jetty.conf %s/solr/jetty.conf_%s" % (dir, dir, st),
                      ctxt=REMOTE,
                      remoteHost=host)
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to execute the copy command on host %s' % host)
            sys.exit(2)
        cmd = Command("Remove old jetty files",
                      "rm -f /tmp/jetty.*")
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to remove old jetty files from /tmp directory. Check /tmp access rights' % (host, dir))
            sys.exit(2)
        cmd = Command("Getting Jetty config to local machine",
                      "scp %s:%s/solr/jetty.conf /tmp/jetty.conf" % (host, dir))
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to load jetty.conf from %s:%s/solr to local machine. Check /tmp directory access rights' % (host, dir))
            sys.exit(2)
        editGPTextConfig(java_opts)
        cmd = Command("Copying Jetty config to target machine",
                      "scp /tmp/jetty.conf_new %s:%s/solr/jetty.conf" % (host, dir))
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to load jetty.conf from local machine to %s:%s/solr. Check the target directory' % (host, dir))
            sys.exit(2)
    return 0

def getGPDBSetting(param):
    cmd = Command("Get GPDB memory parameters",
                  "gpconfig -s %s" % param)
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    mdw_setting, sdw_setting = None, None
    if res.rc <> 0:
        logger.error('Failed to get GPDB setting %s with gpconfig command' % param)
        sys.exit(2)
    else:
        for line in res.stdout.split('\n'):
            line = line.strip()
            if 'Master  value:' in line:
                mdw_setting = line.split(':')[1]
            if 'Segment value:' in line:
                sdw_setting = line.split(':')[1]
    return mdw_setting, sdw_setting

def printGPTextJavaSetting(dburl):
    logger.info('Reading GPText config...')
    query = """
            SELECT  gpsc.hostname as host,
                    pgfse.fselocation as datadir
                FROM pg_tablespace            as pgts,
                     pg_filespace             as pgfs,
                     pg_filespace_entry       as pgfse,
                     gp_segment_configuration as gpsc
            WHERE    pgts.spcfsoid = pgfse.fsefsoid
                AND pgfse.fsefsoid = pgfs.oid
                AND  pgfse.fsedbid = gpsc.dbid
                AND   pgts.spcname = 'pg_default'
                AND    pgfs.fsname = 'pg_system'
                AND   gpsc.content <> -1
            ORDER BY host, datadir
        """
    hostdirs = execute(dburl, query)
    settings = set()
    for hdir in hostdirs:
        host, dir = hdir[0], hdir[1]
        cmd = Command("Get GPText config",
                      "cat %s/solr/jetty.conf | grep JAVA_OPTS" % dir,
                      ctxt=REMOTE,
                      remoteHost=host)
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to execute the cat command on host %s' % host)
            sys.exit(2)
        else:
            r = res.stdout[17:].strip().strip('"')
            settings.add(r)
    if len(settings) > 1:
        logger.warning('GPText memory settings are not consistent among segments. Reconfiguration is recommended')
        logger.warning('Values are: ')
        for s in settings:
            logger.warning('    %s' % s)
    else:
        logger.info ('GPText Memory setting: %s', list(settings)[0])
    return

def main():
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = 'template1',
                         username = 'gpadmin')
    hosts  = getHostList(dburl)
    logger.info ('============= Cluster Parameters ==============')
    logger.info ('List of segment hosts:')
    for h in hosts:
        logger.info ('    %s' % h)
    segnum = getAmountOfHostsPerSegment(dburl)
    logger.info ('Number of segments per host is %d' % segnum)
    masterram = getMasterRam()
    logger.info ('RAM size on master host: %d MB' % masterram)
    logger.info ('RAM size per segment host:')
    ram = getRAMSize(hosts)
    printGPTextJavaSetting(dburl)
    logger.info ('============== Proposed Settings ==============')
    gptext_java = '-Xms120M -Xmx512M -Xloggc:logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=10M'
    logger.info ('GPText Java flags are: %s' % gptext_java)
    if not userinput.ask_yesno(None, "\nContinue with GPText reconfiguration?", 'N'):
        logger.error ('User asked for termination')
        sys.exit(1)
    tuneGPText(dburl, gptext_java)
    logger.info ('Tuning has finished. Now restart GPText to activate it')

main()
