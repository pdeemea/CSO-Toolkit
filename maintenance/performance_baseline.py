import datetime as dt
import sys
import random
try:
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
except ImportError, e:    
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def raise_err(message):
    print 'ERROR: %s' % message
    sys.exit(1)
    
def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-d', '--database',   type='string')
    parser.add_option('-u', '--username',   type='string')
    parser.add_option('-p', '--password',   type='string')
    parser.add_option('-l', '--logfile',    type='string')
    parser.add_option('-n', '--nrows',      type='int')
    
    (options, args) = parser.parse_args()
    if options.help:
        print """
Script executes baseline performance test on the database
Usage:
python performance_baseline.py -d database_name
                               [-u username -p password]
                               [-l logfile]
                               [-n number_of_rows]
    -d | --database   - name of the database to run the test
    -u | --username   - name of the user to be used for testing (default is $PGUSER)
    -p | --password   - password of the user used for testing   (default is $PGPASSWORD)
    -l | --logfile    - performance test output file (default is stdout)
    -n | --nrows      - number of rows generated in test table (default is 5000)
"""
        sys.exit(0)
    if not options.nrows:
        options.nrows = 5000
    if options.nrows < 5000:
        raise_err('Number of rows should be 5000 or more')
    if not options.database:
        raise_err('You must specify database name (-d)')
    if (options.password and not options.username) or (not options.password and options.username):
        raise_err('You should either specify both username and password or not specify them both')
    return options
    
def execute_for_timing(conn, query):
    try:
        n1 = dt.datetime.now()
        curs = dbconn.execSQL(conn, query)
        if query.lower().strip()[:6] == 'select':
            rows = curs.fetchall()
        n2 = dt.datetime.now()
        return ((n2-n1).seconds*1e6 + (n2-n1).microseconds) / 1e6
    except DatabaseError, ex:
        print 'Failed to execute the statement on the database. Please, check log file for errors.'
        print ex
        sys.exit(3)
        
def run_test(dbURL, nrows, outfile):
    prep_queries = [
            ['Preparation Step 1',
             """create temporary table test1 (a bigint, b bigint, c varchar)
                    with (appendonly=true, compresstype=quicklz)
                    on commit drop
                distributed by (a);"""],
            ['Preparation Step 2',
             """insert into test1 (a, b, c)
                    select id, id*2, md5('text' || id::varchar)
                        from generate_series(1,%d) as id;""" % (nrows/64)],
            ['Preparation Step 3',
             """create temporary table test2 (a bigint, b bigint, c numeric, d varchar)
                    with (appendonly=true, compresstype=quicklz)
                    on commit drop
                distributed by (a);"""],
            ['Preparation Step 4',
             """insert into test2 (a, b, c, d)
                    select id*2, id*3, id::numeric*random(), md5('text' || id::varchar)
                        from generate_series(1,%d) as id;""" % (nrows/64)]
        ]
    multiplier = [
            ['Multiplier 1',
             """insert into test1 (a, b, c)
                    select t1.a + t2.max_id,
                           t1.b + t3.max_id,
                           md5('text' || (t1.a + t2.max_id)::varchar)
                        from (select * from test1) as t1,
                             (select max(a) as max_id from test1) as t2,
                             (select max(b) as max_id from test1) as t3;"""],
            ['Multiplier 2',
             """insert into test2 (a, b, c, d)
                    select  t1.a + t2.max_id,
                            t1.b + t3.max_id,
                            (t1.a + t2.max_id)::numeric * random(),
                            md5('text' || ((t1.a + t2.max_id) / 2)::varchar)
                        from (select * from test1) as t1,
                             (select max(a) as max_id from test2) as t2,
                             (select max(b) as max_id from test2) as t3"""]
        ]
    queries = [
            ['Co-located join',
             """select count(*)
                    from (
                        select *
                            from test1 as t1
                                inner join test2 as t2
                                on t1.a = t2.a            
                        ) as q;"""],
            ['Join with single redistribute',
             """select count(*)
                    from (
                        select *
                            from test1 as t1
                                inner join test2 as t2
                                on t1.a = t2.b
                        ) as q;"""],        
            ['Join with 2 redistributions',
             """select count(*)
                    from (
                        select *
                            from test1 as t1
                                inner join test2 as t2
                                on t1.b = t2.b
                        ) as q;"""],
            ['Double redistribution and join on non-unique field',
             """select count(*)
                    from (
                        select *
                            from test1 as t1
                                inner join test2 as t2
                                on t1.b = t2.c::bigint
                        ) as q;"""],
            ['Double redistribution and join on text fields',
             """select count(*)
                    from (
                        select *
                            from test1 as t1
                                inner join test2 as t2
                                on t1.c = t2.d
                        ) as q;"""],
            ['CPU-intensive workload of MD5 hashing',
             """select count(*)
                    from (
                        select md5(a::varchar || '|' || b::varchar || '|' || c::varchar || '|' || d)
                            from test2
                        ) as q;"""],
            ['Sorts and redistributions with all the segments involved',
             """select count(*)
                    from (
                        select  a, b, c, d,
                                max(a) over (partition by (c/100)::bigint) as v1
                            from test2
                        ) as q;"""],
            ['Write test after co-located join',
             """create temporary table test3 
                        with (appendonly=true, compresstype=quicklz)
                        on commit drop
                        as
                    select t1.a, t1.b, t1.c, t2.b as b2, t2.c as c2, t2.d
                        from test1 as t1
                            inner join test2 as t2
                            on t1.a = t2.a;"""],
            ['Write test after redistribution',
             """create temporary table test4
                        with (appendonly=true, compresstype=quicklz)
                        on commit drop
                        as
                    select t1.a, t1.b, t1.c, t2.b as b2, t2.c as c2, t2.d
                        from test1 as t1
                            inner join test2 as t2
                            on t1.b = t2.a;"""],
            ['Cleanup test3 table',
             'drop table test3;'],
            ['Cleanup test4 table',
             'drop table test4;']
        ]
    conn = dbconn.connect(dbURL)
    for q in prep_queries:
        t = execute_for_timing(conn, q[1])
        outfile.write ('%s|%f\n' % (q[0], t))
    # 2 4 8 16 32 64
    for q in multiplier:
        for i in range(6):
            t = execute_for_timing(conn, q[1])
            outfile.write ('%s - run %d|%f\n' % (q[0], i+1, t))
    avg_perf = dict(zip([x[0] for x in queries], [ [] for _ in queries ] ))
    # Average among 3 runs
    for _ in range(3):
        for q in queries:
            t = execute_for_timing(conn, q[1])
            avg_perf[q[0]].append(t)
    for q in queries:
        avg = sum(avg_perf[q[0]]) / len(avg_perf[q[0]])
        outfile.write ('%s|%f\n' % (q[0], avg))
    conn.close()
    return
    
def main():
    options = parseargs()
    outfile = None
    if options.logfile:
        outfile = open(options.logfile, 'w')
    else:
        outfile = sys.stdout
    dbURL = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = options.database,
                         username = options.username,
                         password = options.password)
    nrows = options.nrows
    run_test(dbURL, nrows, outfile)
    if options.logfile:
        outfile.close()
    return

main()
