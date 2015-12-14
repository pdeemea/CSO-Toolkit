select 'drop schema if exists ' || nspname || ' cascade;'
  from (select nspname
          from pg_namespace where nspname like 'pg_temp%'
        except
        select 'pg_temp_' || sess_id::varchar
          from pg_stat_activity) as foo;
 
select 'drop schema if exists ' || nspname || ' cascade;'
  from (select nspname
          from gp_dist_random('pg_namespace')
         where nspname like 'pg_temp%'
        except
        select 'pg_temp_' || sess_id::varchar
          from pg_stat_activity) as foo;