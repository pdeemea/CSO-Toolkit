try:
    from gppylib.db import dbconn
    from gppylib.gplog import *
    from pygresql.pg import DatabaseError
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))
logger = get_default_logger()

# Trigger an error if FullGC time is PARAM_FULLGC_INTERVAL shorter than average
PARAM_FULLGC_INTERVAL=2.0
# Trigger an error if heap size after FullGC event is more than PARAM_FULLGC_HEAP_PERC of total allowed
PARAM_FULLGC_HEAP_PERC=80.0
# Trigger an error if observed more than PARAM_FULLGC_EVENTS for the last hour
PARAM_FULLGC_EVENTS=5
# Trigger an error if total GC time for the last hour is more than PARAM_GC_TIME_SEC
PARAM_GC_TIME_SEC=300.0

# Create temporary table with GC information
QUERY_CLEANUP_TEMP_TABLE="drop table if exists gc_log_lines"
QUERY_CREATE_TEMP_TABLE="""
create temporary table gc_log_lines as
    select  gp_segment,
            gclogline,
            (g).is_full,
            (g).gc_start,
            (g).young_gen_old_size_kb,
            (g).young_gen_new_size_kb,
            (g).young_gen_max_size_kb,
            (g).old_gen_old_size_kb,
            (g).old_gen_new_size_kb,
            (g).old_gen_max_size_kb,
            (g).perm_gen_old_size_kb,
            (g).perm_gen_new_size_kb,
            (g).perm_gen_max_size_kb,
            (g).full_heap_old_size_kb,
            (g).full_heap_new_size_kb,
            (g).full_heap_max_size_kb,
            (g).gc_runtime_sec
        from (
            select  gclogline,
                    gp_segment,
                    gclog.parse_gc_log(gclogline) as g
                from gclog.gc_log_ext
            ) as q
distributed by (gp_segment)"""

# Last Full GC interval is shorter than average
QUERY_FULLGC_INTERVAL="""
with fgc as (
    select  gp_segment,
            gc_start,
            gc_start - lag(gc_start) over (partition by gp_segment order by gc_start) as full_gc_interval
        from (
            select  gp_segment,
                    gc_start
                from gc_log_lines
                where is_full = 1
            ) as q
)
select  f1.gp_segment,
        extract('epoch' from f1.avg_full_gc_interval),
        extract('epoch' from f2.full_gc_interval)
    from (
            select  gp_segment,
                    avg(full_gc_interval) as avg_full_gc_interval
                from fgc
                group by gp_segment
        ) as f1,
        (
            select  gp_segment,
                    full_gc_interval
                from (
                    select  gp_segment,
                            full_gc_interval,
                            row_number() over (partition by gp_segment order by gc_start desc) as rn
                        from fgc
                    ) as q
                where q.rn = 1
        ) as f2
    where f1.gp_segment = f2.gp_segment"""

# Full heap size after the last full GC is greater than X% from max
QUERY_FULLGC_HEAPSIZE="""
select  gp_segment,
        full_heap_max_size_kb,
        full_heap_new_size_kb
    from (
        select  gp_segment,
                full_heap_max_size_kb,
                full_heap_new_size_kb,
                row_number() over (partition by gp_segment order by gc_start) as rn
        from (
            select  gp_segment,
                    gc_start,
                    full_heap_max_size_kb,
                    full_heap_new_size_kb
                from gc_log_lines
                where is_full = 1
            ) as q
        ) as q2
    where rn = 1"""

# Number of Full GC events during the last 1 hour is more than X
QUERY_FULLGC_EVENTS="""
select  gp_segment,
        count(*)
    from gc_log_lines
    where is_full = 1
        and current_timestamp - gc_start < interval '1 hour'
    group by gp_segment"""

# Total time spent in GC during the last 1 hour is more than X
QUERY_GC_TIME="""
select  gp_segment,
        sum(gc_runtime_sec) as gc_total_runtime
    from gc_log_lines
    where current_timestamp - gc_start < interval '1 hour'
    group by gp_segment"""

def execute(conn, query):
    rows = []
    try:
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
    return rows

def execute_noret(conn, query):
    try:
        curs = dbconn.execSQL(conn, query)
        conn.commit()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
    return

def run_tests(dburl):
    conn = dbconn.connect(dburl)
    logger.info ('Creating temporary table with GC information...')
    execute_noret(conn, QUERY_CLEANUP_TEMP_TABLE)
    execute_noret(conn, QUERY_CREATE_TEMP_TABLE)
    logger.info ('Querying GC statistics...')
    gc_intervals = execute(conn, QUERY_FULLGC_INTERVAL)
    gc_heapsizes = execute(conn, QUERY_FULLGC_HEAPSIZE)
    gc_events = execute(conn, QUERY_FULLGC_EVENTS)
    gc_time = execute(conn, QUERY_GC_TIME)
    logger.info('Analyzing...')
    is_error = 0
    logger.info('    Full GC Intervals:')
    for gp_segment, avg_interval, last_interval in gc_intervals:
        logger.info('        Segment %3d: Average %7.1f, Last %7.1f' % (gp_segment, avg_interval, last_interval))
        if last_interval * PARAM_FULLGC_INTERVAL < avg_interval:
            is_error = 1
            logger.error('Last FullGC interval was shorter than accepted portion of avg (%6.2f)' % PARAM_FULLGC_INTERVAL)
    logger.info('    Full GC Heap Sizes:')
    for gp_segment, max_size, last_size in gc_heapsizes:
        logger.info('        Segment %3d: Max %11dKB, Last %11dKB' % (gp_segment, max_size, last_size))
        if float(max_size) * PARAM_FULLGC_HEAP_PERC / 100.0 < float(last_size):
            is_error = 1
            logger.error("Last FullGC didn't reduce GC heap size well enough (%5.1f percent allowed)" % PARAM_FULLGC_HEAP_PERC)
    logger.info('    Full GC Events:')
    for gp_segment, events in gc_events:
        logger.info('        Segment %3d: %4d events' % (gp_segment, events))
        if events > PARAM_FULLGC_EVENTS:
            is_error = 1
            logger.error("Observed more than %d FullGC events within last hour" % PARAM_FULLGC_EVENTS)
    logger.info('    Total GC Time:')
    for gp_segment, gc_seconds in gc_time:
        logger.info('        Segment %3d: %8.2f seconds' % (gp_segment, gc_seconds))
        if gc_seconds > PARAM_GC_TIME_SEC:
            is_error = 1
            logger.error("Observed GC time within last hour is more than %8.2f seconds" % PARAM_GC_TIME_SEC)
    conn.close()
    return is_error

def main():
    logger.info ('GPText GC analysis has started')
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = 'merit',
                         username = 'gpadmin')
    is_error = run_tests(dburl)
    logger.info ('GPText GC analysis has finished')
    if is_error == 1:
        logger.error ('Finished with errors! Check log for details!')
        sys.exit(1)

main()
