create or replace view shared_system.query_locks_all as
    select  current_timestamp::timestamp,
            n.nspname,
            c.relname,
            l.locktype,
            l.transactionid,
            l.pid,
            l.mppsessionid,
            l.mode,
            l.granted,
            a.usename,
            a.query_start,
            a.backend_start,
            a.client_addr,
            substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20) as query_short,
            a.current_query
        from pg_locks as l 
            full join pg_stat_activity as a
            on l.mppsessionid = a.sess_id
            left join pg_class as c
            on l.relation = c.oid
            left join pg_namespace as n
            on c.relnamespace = n.oid
        order by 7,6,2,3;
        
create or replace view shared_system.query_locks_blockers as
    select min(n.nspname)     as table_schema,
           min(c.relname)     as table_name,
           min(l.mode)        as lock_type_blocker,
           min(rb.mode)       as lock_type_waiting,
           min(a.query_start) as lock_start_dttm,               
           current_timestamp - min(a2.query_start) as lock_duration,
           min(substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20))  as blocker_query_short,
           min(substr(regexp_replace(a2.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20)) as waiting_query_short,
           min(a.current_query)  as blocker_query,
           min(a2.current_query) as waiting_query,
           l.relation,
           l.mppsessionid  as session_id_blocker,
           rb.mppsessionid as session_id_waiting
        from pg_locks as l
            inner join (
                select relation, mppsessionid, max(mode) as mode
                    from pg_locks
                    where not granted
                    group by relation, mppsessionid
            ) as rb on l.relation = rb.relation
            inner join pg_stat_activity as a  on a.sess_id  =  l.mppsessionid
            inner join pg_stat_activity as a2 on a2.sess_id = rb.mppsessionid
            inner join pg_class as c on l.relation = c.oid
            inner join pg_namespace as n on c.relnamespace = n.oid
        where l.mppsessionid <> rb.mppsessionid
            and l.granted
        group by l.relation, l.mppsessionid, rb.mppsessionid;
        
create or replace view shared_system.query_locks_blocked as
    select min(n.nspname)     as table_schema,
           min(c.relname)     as table_name,
           min(l.mode)        as blocked_lock_type,
           count(*)           as segments_blocked,
           current_timestamp - min(a.query_start) as lock_waiting_time,
           min(substr(regexp_replace(a.current_query, E'[ \t\r\n]{2}', '', 'g'),1,20))  as waiting_query_short,
           min(a.current_query)  as waiting_query,
           l.relation, l.mppsessionid
        from pg_locks as l
            inner join pg_stat_activity as a  on a.sess_id  =  l.mppsessionid
            inner join pg_class as c on l.relation = c.oid
            inner join pg_namespace as n on c.relnamespace = n.oid
        where not l.granted
        group by l.relation, l.mppsessionid;
        
/*
alias locked="psql -c 'select table_schema, table_name, blocked_lock_type, segments_blocked, lock_waiting_time, waiting_query_short from shared_system.query_locks_blocked'"
alias locks="psql -c 'select table_schema, table_name, blocked_lock_type, lock_start_dttm, lock_waiting_time, blocked_query_short, waiting_query_short from shared_system.query_locks_blockers;'"
*/




