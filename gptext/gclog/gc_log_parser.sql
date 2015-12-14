create schema gclog;

drop type if exists gclog.gclogline cascade;
create type gclog.gclogline as (
    is_full smallint,
    gc_start timestamp,
    young_gen_old_size_kb int,
    young_gen_new_size_kb int,
    young_gen_max_size_kb int,
    old_gen_old_size_kb int,
    old_gen_new_size_kb int,
    old_gen_max_size_kb int,
    perm_gen_old_size_kb int,
    perm_gen_new_size_kb int,
    perm_gen_max_size_kb int,
    full_heap_old_size_kb int,
    full_heap_new_size_kb int,
    full_heap_max_size_kb int,
    gc_runtime_sec float8
);

create or replace function gclog.parse_gc_log(logline varchar) returns gclog.gclogline as $BODY$
import re
res = {}
res['is_full'] = None
res['gc_start'] = None
res['young_gen_old_size_kb'] = None
res['young_gen_new_size_kb'] = None
res['young_gen_max_size_kb'] = None
res['old_gen_old_size_kb'] = None
res['old_gen_new_size_kb'] = None
res['old_gen_max_size_kb'] = None
res['perm_gen_old_size_kb'] = None
res['perm_gen_new_size_kb'] = None
res['perm_gen_max_size_kb'] = None
res['full_heap_old_size_kb'] = None
res['full_heap_new_size_kb'] = None
res['full_heap_max_size_kb'] = None
res['gc_runtime_sec'] = None
m_list = [
    [0, re.compile('(?P<dt>[0-9]{4}-[0-9]{2}-[0-9]{2})T(?P<tm>[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)[\+\-][0-9]+\:\s+[0-9]+\.[0-9]+:\s+\[GC \[PSYoungGen: (?P<ygos>[0-9]+)K->(?P<ygns>[0-9]+)K\((?P<ygms>[0-9]+)K\)\]\s+(?P<fhos>[0-9]+)K->(?P<fhns>[0-9]+)K\((?P<fhms>[0-9]+)K\),\s+(?P<gs>[0-9]+\.[0-9]+)\s+secs\]')],
    [0, re.compile('(?P<dt>[0-9]{4}-[0-9]{2}-[0-9]{2})T(?P<tm>[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)[\+\-][0-9]+\:\s+[0-9]+\.[0-9]+:\s+\[GC\s+[0-9]+\.[0-9]+:\s+\[DefNew: (?P<ygos>[0-9]+)K->(?P<ygns>[0-9]+)K\((?P<ygms>[0-9]+)K\)\,\s+[0-9]+\.[0-9]+\s+secs\]\s+(?P<fhos>[0-9]+)K->(?P<fhns>[0-9]+)K\((?P<fhms>[0-9]+)K\),\s+(?P<gs>[0-9]+\.[0-9]+)\s+secs\]')],
    [1, re.compile('(?P<dt>[0-9]{4}-[0-9]{2}-[0-9]{2})T(?P<tm>[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)[\+\-][0-9]+\:\s+[0-9]+\.[0-9]+:\s+\[Full GC\s+\[PSYoungGen: (?P<ygos>[0-9]+)K->(?P<ygns>[0-9]+)K\((?P<ygms>[0-9]+)K\)\]\s+\[PSOldGen: (?P<ogos>[0-9]+)K->(?P<ogns>[0-9]+)K\((?P<ogms>[0-9]+)K\)\]\s+(?P<fhos>[0-9]+)K->(?P<fhns>[0-9]+)K\((?P<fhms>[0-9]+)K\)\s+\[PSPermGen: (?P<pgos>[0-9]+)K->(?P<pgns>[0-9]+)K\((?P<pgms>[0-9]+)K\)\],\s(?P<gs>[0-9]+\.[0-9]+)\s+secs\]')]
    ]
for mi in m_list:
    m = mi[1].match(logline)
    if m:
        is_full = mi[0]
        res['is_full'] = is_full
        res['gc_start'] = m.group('dt') + ' ' + m.group('tm')
        res['young_gen_old_size_kb'] = int(m.group('ygos'))
        res['young_gen_new_size_kb'] = int(m.group('ygns'))
        res['young_gen_max_size_kb'] = int(m.group('ygms'))
        res['full_heap_old_size_kb'] = int(m.group('fhos'))
        res['full_heap_new_size_kb'] = int(m.group('fhns'))
        res['full_heap_max_size_kb'] = int(m.group('fhms'))
        res['gc_runtime_sec'] = float(m.group('gs'))
        if is_full == 1:
            res['old_gen_old_size_kb'] = int(m.group('ogos'))
            res['old_gen_new_size_kb'] = int(m.group('ogns'))
            res['old_gen_max_size_kb'] = int(m.group('ogms'))
            res['perm_gen_old_size_kb'] = int(m.group('pgos'))
            res['perm_gen_new_size_kb'] = int(m.group('pgns'))
            res['perm_gen_max_size_kb'] = int(m.group('pgms'))
return res
$BODY$
language plpythonu
immutable;

drop external table if exists gclog.gc_log_ext;
create external web table gclog.gc_log_ext (
    gclogline varchar,
    gp_segment int
)
execute 'cat $GP_SEG_DATADIR/solr/logs/gc.log* | xargs -i echo "{}|$GP_SEGMENT_ID"' on all
format 'text' (delimiter '|')
segment reject limit 1000000;
