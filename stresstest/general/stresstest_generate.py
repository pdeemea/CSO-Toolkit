import sys
import time
from datetime import datetime
try:
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def execute(dburl, query):
    res = [['']]
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
        res = rows
    except Exception as ex:
        sys.stderr.write ('Exception during execute: %s' % str(ex))
        pass
    return res

class WorkloadGenerator:
    input_filename  = ''
    output_filename = ''
    dburl           = None
    tables          = []
    
    def __init__(self, filename_in, filename_out, dburl_in):
        self.input_filename  = filename_in
        self.output_filename = filename_out
        self.dburl           = dburl_in
        self.tables          = []
        return
    
    def get_tables(self):
        f = open(self.input_filename, 'r')
        for t in f:
            self.tables.append(t.strip())
        return

    def get_table_stats(self):
        query = """
            select a.attname, s.stadistinct
                from pg_class as c,
                     pg_namespace as n,
                     pg_statistic as s,
                     pg_attribute as a
                where c.relnamespace = n.oid
                    and s.starelid = c.oid
                    and a.attrelid = c.oid
                    and a.attnum = s.staattnum
                    and n.nspname || '.' || c.relname = '%s'
                order by s.stadistinct desc
        """
        tables_out = dict()
        for t in self.tables:
            res = execute(self.dburl, query % t)
            if len(res) >= 2:
                tables_out[t] = res
            else:
                print 'ERROR: Table %s does not have enough columns. %d columns returned' % (t, len(res))
        self.tables = tables_out
        return

    def get_load_disk(self):
        queries = []
        for t in self.tables:
            queries.append('select max(%s) from %s;' % (self.tables[t][0][0], t))
        return queries

    def get_load_cpu(self):
        queries = []
        for t in self.tables:
            query = "set statement_mem='1500MB'; select count(distinct md5(%s)) from %s;"
            md5calc = '||'.join([f[0]+'::varchar' for f in self.tables[t]])
            queries.append(query % (md5calc, t))
        return queries

    def get_load_network(self):
        queries = []
        for t in self.tables:
            query = "set statement_mem='1500MB'; select count(*) from (select count(distinct %s) over (partition by %s) from %s) as q;" % (
                    self.tables[t][1][0],
                    self.tables[t][0][0],
                    t)
            queries.append(query)
        return queries

    def get_load_memory(self):
        queries = []
        for t in self.tables:
            query = ''
            key   = ''
            for i in range(len(self.tables[t])):
                f = self.tables[t][i][0]
                if i == 0:
                    query  = "set statement_mem='1500MB'; select count(*) from (select %s" % f
                    key    = f
                else:
                    query += ', row_number() over (partition by %s order by %s)' % (key, f)
                if i > 5:
                    break
            query += 'from %s) as q;' % t
            queries.append(query)
        return queries
    
    def dump_queries(self, qdisk, qcpu, qnet, qmem):
        def output (f, desc, queries):
            for i in range(len(queries)):
                f.write('%s!%d!%s\n' % (desc, i, queries[i]))
            return
        f = open(self.output_filename, 'w')
        output(f, 'disk', qdisk)
        output(f, 'cpu', qcpu)
        output(f, 'network', qnet)
        output(f, 'memory', qmem)
        return
        
    def run (self):
        self.get_tables()
        self.get_table_stats()
        qdisk  = self.get_load_disk()
        qcpu   = self.get_load_cpu()
        qnet   = self.get_load_network()
        qmem   = self.get_load_memory()
        self.dump_queries(qdisk, qcpu, qnet, qmem)

def main():
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = sys.argv[1],
                         username = 'gpadmin')
    wg = WorkloadGenerator(sys.argv[2], sys.argv[3], dburl)
    wg.run()
    return
    
main()

# python stresstest_generate.py dssprod conf/stresstest_table_list_big.csv conf/stresstest_queries_big.sql
