import sys
import time
from datetime import datetime
try:
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def executeForSingleInt(dburl, query):
    res = -1
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
        res = int(rows[0][0])
    except Exception as ex:
        sys.stderr.write ('Exception during execute: %s' % str(ex))
        pass
    return res

def collectStat():
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = sys.argv[1],
                         username = 'gpadmin')
    pgav  = executeForSingleInt (dburl, "set gp_select_invisible=off; select count(*) from pg_attribute;")
    pgat  = executeForSingleInt (dburl, "set gp_select_invisible=on; select count(*) from pg_attribute;")
    pgcv  = executeForSingleInt (dburl, "set gp_select_invisible=off; select count(*) from pg_class;")
    pgct  = executeForSingleInt (dburl, "set gp_select_invisible=on; select count(*) from pg_class;")
    pgnv  = executeForSingleInt (dburl, "set gp_select_invisible=off; select count(*) from pg_namespace;")
    pgnt  = executeForSingleInt (dburl, "set gp_select_invisible=on; select count(*) from pg_namespace;")
    sys.stdout.write('%s|%d|%d|%d|%d|%d|%d\n' % (ts, pgav, pgat, pgcv, pgct, pgnv, pgnt))
    sys.stdout.flush()

sys.stdout.write('Timestamp|Rows pg_attribute visible|Rows pg_attribute total|Rows pg_class visible|Rows pg_class total|Rows pg_namespace visible|Rows pg_namespace total\n')
while True:
    try:
        collectStat()
        time.sleep(600)
    except Exception as ex:
        sys.stderr.write ('Exception during main cycle: %s' % str(ex))
        pass
