# Part of the memory allocated to GPDB and GPText
# Correct values are from 0.1 to 1.0
allocation_rate = 0.9

# Memory split between GPDB and GPText
# Correct values are from 0.1 to 0.9
# The greater value, the more memory is dedicated to GPText
memory_split = 0.5

# CPU split between GPDB and GPText
# Correct values are from 0.3 (GPDB can utilize 30% of CPU) to 2.0 (each GPDB process can utilize 2x CPU segment system have)
# The greater value, the more CPU are dedicated to GPDB
cpu_split = 0.7

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

def parseargs():
    global allocation_rate, memory_split, cpu_split
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')    
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-f', '--force',      action='store_true')
    parser.add_option('-s', '--memsplit',   type='float')
    parser.add_option('-a', '--allocrate',  type='float')
    parser.add_option('-c', '--cpusplit',   type='float')
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script configures memory and CPU allocation for GPText and GPDB. GPDB should be running when
the script is started. The script should work on master server. Local GPDB connection under
gpadmin to template1 database should be passwordless.
Usage:
python gptext_tune_settings.py [-s memory_split] [-a allocation_rate] [-c cpu_split] [-f | --force]
    memory_split    - [0.1 .. 0.9] - split of the memory between GPText and GPDB. Greater value - more memory for GPText
    allocation_rate - [0.1 .. 0.9] - part of the system memory available to GPText and GPDB
    cpu_split       - [0.3 .. 2.0] - part of the CPU dedicated to GPDB. Over utilization is allowed
    force           - do not ask for confirmation of changing the memory settings
"""
        sys.exit(0)        
    if options.allocrate:
        if not (options.allocrate >= 0.1 and options.allocrate <= 0.9):
            logger.error('Correct values for --allocrate are [0.1 .. 0.9]')
            sys.exit(3)
        allocation_rate = options.allocrate
    if options.memsplit:
        if not (options.memsplit >= 0.1 and options.memsplit <= 0.9):
            logger.error('Correct values for --memsplit are [0.1 .. 0.9]')
            sys.exit(3)
        memory_split = options.memsplit
    if options.cpusplit:
        if not (options.cpusplit >= 0.3 and options.cpusplit <= 2.0):
            logger.error('Correct values for --cpusplit are [0.3 .. 2.0]')
            sys.exit(3)
        cpu_split = options.cpusplit
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

def getMasterCPUCoreNumber():
    cmd = Command("Getting amount of free memory",
                  "cat /proc/cpuinfo | grep processor | wc -l")
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    if res.rc <> 0:
        logger.error('Failed to execute the statement "cat /proc/cpuinfo" on the remote host %s' % host)
        sys.exit(2)
    else:
        r = int(res.stdout.strip())
    return r
    
def getCPUCoreNumber(hosts):
    corenum = None
    for host in hosts:
        cmd = Command("Getting amount of free memory",
                      "cat /proc/cpuinfo | grep processor | wc -l",
                      ctxt=REMOTE,
                      remoteHost=host)
        cmd.run(validateAfter=True)
        res = cmd.get_results()
        if res.rc <> 0:
            logger.error('Failed to execute the statement "cat /proc/cpuinfo" on the remote host %s' % host)
            sys.exit(2)
        else:
            r = int(res.stdout.strip())
            logger.info ('    %s - %d Cores' % (host, r))
            if corenum is None:
                corenum = r
            else:
                if corenum <> r:
                    logger.error ('Segment configuration is not symmetric. All the segments should have equal amount of CPU cores')
                corenum = min(corenum, r)
    return corenum

def tuneGPDBCPU (cpu_segment, cpu_master):
    logger.info ('Tuning GPDB CPU (gp_resqueue_priority_cpucores_per_segment)...')
    cmd = Command("Setting GPDB memory limit",
                  "gpconfig -c gp_resqueue_priority_cpucores_per_segment -v %d -m %d" % (cpu_segment, cpu_master))
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    if res.rc <> 0:
        logger.error('Failed to set gp_resqueue_priority_cpucores_per_segment with gpconfig command')
        sys.exit(2)
    return

def tuneGPDBRAM (ram_size, master_ram):
    logger.info ('Tuning GPDB RAM (gp_vmem_protect_limit)...')
    cmd = Command("Setting GPDB memory limit",
                  "gpconfig -c gp_vmem_protect_limit -v %.1f -m %d" % (ram_size, master_ram))
    cmd.run(validateAfter=True)
    res = cmd.get_results()
    if res.rc <> 0:
        logger.error('Failed to set gp_vmem_protect_limit with gpconfig command')
        sys.exit(2)
    return

def editGPTextConfig(ram_size):
    fi = open('/tmp/jetty.conf', 'r')
    fo = open('/tmp/jetty.conf_new', 'w')
    for line in fi:
        if not 'JAVA_OPTS' in line:
            fo.write(line)
        else:
            fo.write('export JAVA_OPTS="-Xms1024M -Xmx%dM"\n' % ram_size)
    fi.close()
    fo.close()
    return
    
def tuneGPTextRAM (dburl, ram_size):
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
        editGPTextConfig(ram_size)
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

def printGPTextMemSetting(dburl):
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
            r = res.stdout.split('=')[1].strip().strip('"').split(' ')[1]
            settings.add(r)
    if len(settings) > 1:
        logger.warning('GPText memory settings are not consistent among segments. Reconfiguration is recommended')
        logger.warning('Values are: %s', ' '.join(list(settings)))
    else:
        logger.info ('GPText Memory setting: %s', list(settings)[0])
    return
    
def printCurrentSettings(dburl):
    gp_mem_mdw, gp_mem_sdw = getGPDBSetting('gp_vmem_protect_limit')
    logger.info ('GPDB Memory limits (MB):')
    logger.info ('    master:  %6d', int(gp_mem_mdw))
    logger.info ('    segment: %6d', int(gp_mem_sdw))
    gp_cpu_mdw, gp_cpu_sdw = getGPDBSetting('gp_resqueue_priority_cpucores_per_segment')
    logger.info ('GPDB CPU usage:')
    logger.info ('    master:  %6.1f', float(gp_cpu_mdw))
    logger.info ('    segment: %6.1f', float(gp_cpu_sdw))
    printGPTextMemSetting(dburl)
    
def main():
    options = parseargs()
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = 'template1',
                         username = 'gpadmin')
    hosts  = getHostList(dburl)
    logger.info ('========== Cluster Hardware Parameters =========')
    logger.info ('List of segment hosts:')
    for h in hosts:
        logger.info ('    %s' % h)
    segnum = getAmountOfHostsPerSegment(dburl)
    logger.info ('Number of segments per host is %d' % segnum)
    masterram = getMasterRam()
    logger.info ('RAM size on master host: %d MB' % masterram)
    logger.info ('RAM size per segment host:')
    ram = getRAMSize(hosts)
    mastercores = getMasterCPUCoreNumber()
    logger.info ('Number of CPU cores on master host: %d' % mastercores)
    logger.info ('Number of CPU cores per segment host:')
    cores = getCPUCoreNumber(hosts)
    logger.info ('========== Current Greenplum Settings ==========')
    printCurrentSettings(dburl)
    logger.info ('========== Tune Application Constants ==========')
    logger.info ('Part of the host memory allocated to GPDB and GPText: %.2f' % allocation_rate)
    logger.info ('Memory split between GPDB and GPText: %.2f' % memory_split)
    logger.info ('Part of the CPU allocated to GPDB: %.2f' % cpu_split)
    logger.info ('============== Proposed  Settings ==============')
    usable_ram = int(ram*allocation_rate)
    logger.info ('Amount of memory allocated to GPDB and GPText: %d MB' % usable_ram)
    gptext_ram = int(float(usable_ram)*memory_split)/segnum/2
    logger.info ('GPText memory setting is: %d MB' % gptext_ram)
    if gptext_ram < 1000:
        logger.error ('GPText has less than 1GB of RAM per process. This configuration will not work. Reduce number of segments or adjust memory split')
        sys.exit(1)
    if gptext_ram < 2000:
        logger.warning ('GPText has less than 2GB of RAM per process. This configuration is not recommended')
    gpdb_ram   = int(float(usable_ram)*(1.0 - memory_split))/segnum
    logger.info ('GPDB memory setting is %d MB for segment and %d MB for master' % (gpdb_ram, int(float(masterram)*allocation_rate)))
    if gpdb_ram < 2000:
        logger.error ('GPDB has less than 2GB of RAM per primary segment. This configuration will not work. Reduce number of segments or adjust memory split')
        sys.exit(1)
    if gpdb_ram < 4000:
        logger.warning ('GPDB has less than 4GB of RAM per primary segment. This configuration is not recommended')
    cpu_segment = float(cores) / float(segnum) * cpu_split
    cpu_master  = min(24, max(1, mastercores - 1))
    logger.info ('GPDB cpu cores per segment is %.1f' % cpu_segment)
    logger.info ('GPDB cpu cores for master  is %d' % cpu_master)
    if cpu_segment < 1.0:
        logger.error ('GPDB has less than 1 CPU core per each primary segment. This configuration will not work. Reduce number of segments or adjust CPU split')
        sys.exit(1)
    if cpu_segment < 2.0:
        logger.warning ('GPDB has less than 2 CPU cores per each primary segment. This configuration is not recommended')
    if not options.force:
        if not userinput.ask_yesno(None, "\nContinue with memory and cpu reconfiguration?", 'N'):
            logger.error ('User asked for termination')
            sys.exit(1)
    tuneGPDBCPU  (cpu_segment, cpu_master)
    tuneGPDBRAM  (gpdb_ram, int(float(masterram)*allocation_rate))
    tuneGPTextRAM(dburl, gptext_ram)
    logger.info ('Tuning finished. Now restart GPDB and GPText to activate it')
    
main()