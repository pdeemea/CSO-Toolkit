import sys
try:
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
#    from gppylib.db import dbconn
    from gppylib.gplog import *
#    from gppylib import userinput
#    from pygresql.pg import DatabaseError
#    from gppylib.commands.base import Command, REMOTE, ExecutionError
except ImportError, e:    
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))
logger = get_default_logger()

def raise_err (message):
    logger.error (message)
    sys.exit(3)

def parseargs():
    global allocation_rate, memory_split, cpu_split
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help',  action='store_true')
    parser.add_option('-s', '--seghosts',    type='int')
    parser.add_option('-d', '--segdbs',      type='int')
    parser.add_option('-p', '--primarydirs', type='string')
    parser.add_option('-m', '--mirrordirs',  type='string')
    parser.add_option('-o', '--outfile',     type='string')
    parser.add_option('-i', '--initsystem',  action='store_true')
    parser.add_option('-e', '--expansion',   action='store_true')
    parser.add_option('-a', '--added',       type='int')
    parser.add_option('-C', '--maxcontent',  type='int')
    parser.add_option('-D', '--maxdbid',     type='int')
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script generates the segment placement map for Greenplum initialization.
By default the primary segment directories are /data1/primary and /data2/primary
and the mirror directories are /data1/mirror and /data2/mirror. Output file is
by default located in the working directory and is called gpinitsystem_map_<timestamp>
Usage:
python generate_segment_map.py -s number_of_segment_hosts -d number_of_dbs_per_host
                              [-o outfile]
                              [-p directories_for_primaries -m directories_for_mirrors]
                              [-i | --initsystem]
                              [-e -a number_of_hosts_added --maxcontent max_content
                                    --maxdbid max_dbid]
    -s | --seghosts    - Number of segment hosts in the system
    -d | --segdbs      - Number of segment databases per host
    -o | --outfile     - Output file
    -p | --primarydirs - Colon-separated list of primary segment directories
    -m | --mirrordirs  - Colon-separated list of mirror segment directories
    -i | --initsystem  - Generate map file for system initialization
    -e | --expansion   - Generate map file for system expansion
    -a | --added       - Number of segment hosts added during expansion
    -C | --maxcontent  - Maximal number of content in existing GPDB
    -D | --maxdbid     - Maximal number of dbid in existing GPDB
Examples:
    1. Initialize system with 16 segment servers and 4 segments per host:
    python generate_segment_map.py -s 16 -d 4 -i > gpinitsystem_map
    2. Prepare expansion map to add 8 segment servers to existing system with
        16 segment servers and 4 segment databases per host:
    python generate_segment_map.py -s 16 -d 4 -e -a 8 --maxcontent 100 --maxdbid 100 > gpexpand_map
"""
        sys.exit(0)
    if not options.seghosts or not options.segdbs:
        raise_err('You must specify both number of segment hosts (-s) and number of segment databases on each host (-d)')
    if (options.primarydirs and not options.mirrordirs) or (not options.primarydirs and options.mirrordirs):
        raise_err('You must either specify both folders for primaries and mirrors or use defaults for both')
    if (not options.initsystem and not options.expansion) or (options.initsystem and options.expansion):
        raise_err('You should either specify init system mode ( -i ) or expansion mode ( -e )')
    if options.expansion and not options.added:
        raise_err('In expansion mode you must specify number of segment servers added')
    return options

def validate_options(options):
    seghosts = int(options.seghosts)
    segdbs   = int(options.segdbs)
    num_prim = 2
    if options.primarydirs:
        num_prim = options.primarydirs.count(':') + 1
        num_mirr = options.mirrordirs.count(':')  + 1
        if num_prim <> num_mirr:
            raise_err('Number of directories for primaries (%d) should be the same as the number of directories for mirrors (%d)' % (num_prim, num_mirr))
        prim = set(options.primarydirs.split(':'))
        mirr = set(options.mirrordirs.split(':'))
        if len(prim & mirr) > 0:
            raise_err('Primaries and mirrors cannot be put to the same directory. The overlapping ones are: %s' % ', '.join([x for x in prim & mirr]))
        if len(prim) != num_prim:
            raise_err('List of primary directories contain non-unique entries, while only unique entries are allowed')
        if len(mirr) != num_mirr:
            raise_err('List of mirror directories contain non-unique entries, while only unique entries are allowed')
    if (seghosts >= 4 and seghosts % 4 != 0 and options.initsystem):
        raise_err('This script creates initialization map for GPDB with redundancy groups of 4 segment hosts. The amount of servers you passed (%d) cannot be splitted in a number of groups of 4 servers' % seghosts)
    if (seghosts < 4 and options.expansion):
        raise_err('Cannot expand the system with less than 4 servers')
    if options.expansion:
        addedhosts = int(options.added)
        if (options.expansion and addedhosts % 4 != 0):
            raise_err('You can expand the system only with a chunks of 4 servers, now trying to make it run with %d segment servers added' % addedhosts)
    if segdbs % num_prim != 0:
        raise_err('%d segment databases cannot be evenly distributed in %d directories' % (segdbs, num_prim))
    return

def find_min(mirrors, mir_prim, j):
    mir_len = [ len(x) for x in mirrors ]
    xmax, pmax, xi = 100, 100, 0
    for i in range(4):
        if i <> j:
            if (xmax > mir_len[i]) or (xmax == mir_len[i] and pmax > mir_prim[j][i]):
                xmax, pmax, xi = mir_len[i], mir_prim[j][i], i
    return xi

def generate_small_cluster(seghosts, host, dbid, content, segdbs, primarydirs, mirrordirs):
    def find_min_simple(mirrors, seghosts, j):
        mir_len = [ len(x) for x in mirrors ]
        xmax, xi = 100, 0
        for i in range(seghosts):
            if i <> j:
                if (xmax > mir_len[i]):
                    xmax, xi = mir_len[i], i
        return xi
    hostnames = [ 'sdw' + str(x) for x in range(host, host+4) ]
    primaries = [ [] for x in range(seghosts) ]
    for i in range(seghosts):
        for j in range(segdbs):
            primaries[i].append(
                # host, dbid, content, port, replication_port, directory
                [ hostnames[i], dbid, content, 1025+j, 1089+j, primarydirs[j%len(primarydirs)] + '/gpseg' + str(content) ]
                )
            dbid    += 1
            content += 1
    mirrors  = [ [] for x in range(seghosts) ]
    for i in range(segdbs):
        for j in range(seghosts):
            cont = primaries[j][i][2]
            m = find_min_simple(mirrors, seghosts, j)
            mirrors[m].append(
                # host, dbid, content, port, replication_port, directory
                [ hostnames[m], dbid, cont, 1153+i, 1217+i, mirrordirs[i%len(mirrordirs)] + '/gpseg' + str(cont) ]
                )
            dbid    += 1
    return primaries, mirrors
    
def generate_next_4_hosts (host, dbid, content, segdbs, primarydirs, mirrordirs):
    hostnames = [ 'sdw' + str(x) for x in range(host, host+4) ]
    primaries = [ [], [], [], [] ]
    for i in range(4):
        for j in range(segdbs):
            primaries[i].append(
                # host, dbid, content, port, replication_port, directory
                [ hostnames[i], dbid, content, 1025+j, 1089+j, primarydirs[j%len(primarydirs)] + '/gpseg' + str(content) ]
                )
            dbid    += 1
            content += 1
    mirrors  = [ [], [], [], [] ]
    mir_prim = [ [0 for x in range(4)] for y in range(4) ]
    for i in range(segdbs):
        for j in range(4):
            cont = primaries[j][i][2]
            m = find_min(mirrors, mir_prim, j)
            mirrors[m].append(
                # host, dbid, content, port, replication_port, directory
                [ hostnames[m], dbid, cont, 1153+i, 1217+i, mirrordirs[i%len(mirrordirs)] + '/gpseg' + str(cont)]
                )
            mir_prim[j][m] += 1
            dbid    += 1
    return primaries, mirrors, dbid, content
    
def generate_map (options):
    if options.primarydirs:
        primarydirs = options.primarydirs.split(':')
        mirrordirs  = options.mirrordirs.split(':')
    else:
        primarydirs = ['/data1/primary', '/data2/primary']
        mirrordirs  = ['/data1/mirror',  '/data2/mirror']
    if options.initsystem:
        host     = 1
        dbid     = 2
        content  = 0
        seghosts = int(options.seghosts)
        segdbs   = int(options.segdbs)
    if options.expansion:
        host     = int(options.seghosts)   + 1
        dbid     = int(options.maxdbid)    + 1
        content  = int(options.maxcontent) + 1
        seghosts = host + int(options.added)
        segdbs   = int(options.segdbs)
    resmap_prim = []
    resmap_mirr = []
    if seghosts < 4:
        resmap_prim, resmap_mirr = generate_small_cluster(seghosts, host, dbid, content, segdbs, primarydirs, mirrordirs)
    else:
        while host < seghosts:
            new_prim, new_mirr, dbid, content = generate_next_4_hosts(host, dbid, content, segdbs, primarydirs, mirrordirs)
            resmap_prim.extend(new_prim)
            resmap_mirr.extend(new_mirr)
            host = host + 4
    return resmap_prim, resmap_mirr

def output_map (options, prim, mirr):
    if options.outfile:
        f = open(options.outfile, 'w')
    else:
        f = sys.stdout
    if options.initsystem:
        f.write (
"""ARRAY_NAME="Greenplum DCA"

TRUSTED_SHELL=ssh

CHECK_POINT_SEGMENTS=8

ENCODING=unicode

QD_PRIMARY_ARRAY=mdw1:5432:/data/master/gpseg-1:1:-1:0

declare -a PRIMARY_ARRAY=(
""")
        for host in prim:
            for seg in host:
                r = [ seg[0], seg[3], seg[5], seg[1], seg[2], seg[4] ]
                f.write('%s:%d:%s:%d:%d:%d\n' % tuple(r))
        f.write (
""")

declare -a MIRROR_ARRAY=(
""")
        for host in mirr:
            for seg in host:
                r = [ seg[0], seg[3], seg[5], seg[1], seg[2], seg[4] ]
                f.write('%s:%d:%s:%d:%d:%d\n' % tuple(r))
        f.write(')\n')
    if options.expansion:
        for host in prim:
            for seg in host:
                r = [ seg[0], seg[0], seg[3], seg[5], seg[1], seg[2], seg[4] ]
                f.write ('%s:%s:%d:%s:%d:%d:p:%d\n' % tuple(r))
        for host in mirr:
            for seg in host:
                r = [ seg[0], seg[0], seg[3], seg[5], seg[1], seg[2], seg[4] ]
                f.write ('%s:%s:%d:%s:%d:%d:m:%d\n' % tuple(r))
    return

def main():
    options = parseargs()
    validate_options(options)
    prim, mirr = generate_map(options)
    output_map(options, prim, mirr)

main()