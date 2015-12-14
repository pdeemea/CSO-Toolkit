drop external table if exists ext_processes cascade;
CREATE EXTERNAL WEB TABLE ext_processes (
    segment_id varchar,
    command varchar
)
execute E'ps -ewwopid,ppid,rss,vsz,pmem,pcpu,time,etime,start_time,wchan,stat,psr,args | grep postgres | grep "port $GP_SEG_PORT" | awk ''{ print ENVIRON["GP_SEGMENT_ID"] "|" $"@" }'' ' on all
format 'text' (delimiter '|');

create or replace function parse_ps_output (s varchar) returns varchar[] as $BODY$
import re
res = None
if not ('logger process' in s
        or 'primary process' in s
        or 'primary receiver ack process' in s
        or 'primary sender process' in s
        or 'primary consumer ack process' in s
        or 'primary recovery process' in s
        or 'primary verification process' in s
        or 'stats collector process' in s
        or 'writer process' in s
        or 'checkpoint process' in s
        or 'sweeper process' in s):
    m = re.match(r'([0-9]+)\s+[0-9]+\s+([0-9]+)\s+([0-9]+)\s+([0-9\.]+)\s+([0-9\.]+) .* postgres: port\s+[0-9]+, (\w+) (\w+) .* con([0-9]+) seg([0-9]+) cmd([0-9]+) (slice[0-9]+)?', s)
    if m:
        res = [
            m.group(1),  # PID
            m.group(2),  # RSS
            m.group(3),  # VSZ
            m.group(4),  # MEM
            m.group(5),  # CPU
            m.group(6),  # Username
            m.group(7),  # Database
            m.group(8),  # Session ID
            m.group(9),  # Segment ID
            m.group(10), # Command ID
            m.group(11)  # Slice ID
        ]
return res
$BODY$
language plpythonu
volatile;

drop table if exists mem_watcher;
create table mem_watcher (
    ts timestamp,
    segment_id int,
    pid int,
    rss bigint,
    vsz bigint,
    mem float8,
    cpu float8,
    username varchar,
    database varchar,
    session_id int,
    command_id int,
    slice varchar
)
with (appendonly=true, compresstype=zlib, compresslevel=9)
distributed by (segment_id);

create or replace view mem_watcher_view as
    select  current_timestamp,
            segment_id::int,
            ps[1]::int      as pid,
            ps[2]::bigint   as rss,
            ps[3]::bigint   as vsz,
            ps[4]::float8   as mem,
            ps[5]::float8   as cpu,
            ps[6]::varchar  as username,
            ps[7]::varchar  as database,
            ps[8]::int      as session_id,
            ps[10]::int     as command_id,
            ps[11]::varchar as slice
        from (
            select  segment_id,
                    parse_ps_output(command) as ps
                from ext_processes
            ) as q
        where ps is not null;

insert into mem_watcher select * from mem_watcher_view;

/*
select  session_id,
        command_id,
        (max(real_mem_mb)-min(real_mem_mb))/avg(real_mem_mb) as real_mem_skew,
        (max(virtual_mem_mb)-min(virtual_mem_mb))/avg(virtual_mem_mb) as virtual_mem_skew,
        max(real_mem_mb) as max_real_mem_mb,
        min(real_mem_mb) as min_real_mem_mb,
        max(virtual_mem_mb) as max_virtual_mem_mb,
        min(virtual_mem_mb) as min_virtual_mem_mb
    from (
        select  segment_id,
                session_id,
                command_id,
                max(real_mem_mb)    as real_mem_mb,
                max(virtual_mem_mb) as virtual_mem_mb
            from (
                select  ts,
                        segment_id,
                        session_id,
                        command_id,
                        rss/1024. as real_mem_mb,
                        vsz/1024. as virtual_mem_mb
                    from mem_watcher
                    where command_id is not null
                ) as q
            group by 1,2,3
        ) as q2
    group by 1,2
    order by 5 desc
    limit 1000;
*/

#!/bin/bash

for i in {1..100000}; do
    psql -d gpperfmon -c 'insert into mem_watcher select * from mem_watcher_view;'
    sleep 60
done
