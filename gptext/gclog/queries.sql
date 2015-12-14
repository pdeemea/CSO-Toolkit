-- Create temporary table with GC information
drop table if exists gc_log_lines;
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
distributed by (gp_segment);

-- Last Full GC interval is shorter than average
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
        f1.avg_full_gc_interval,
        f2.full_gc_interval
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
    where f1.gp_segment = f2.gp_segment;

-- Full heap size after the last full GC is greater than X% from max
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
    where rn = 1;

-- Number of Full GC events during the last 1 hour is more than X
select  gp_segment,
        count(*)
    from gc_log_lines
    where is_full = 1
        and current_timestamp - gc_start < interval '1 hour'
    group by gp_segment;

-- Total time spent in GC during the last 1 hour is more than X
select  gp_segment,
        sum(gc_runtime_sec) as gc_total_runtime
    from gc_log_lines
    where current_timestamp - gc_start < interval '1 hour'
    group by gp_segment;
