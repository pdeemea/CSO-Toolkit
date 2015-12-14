create table test1 (a int, b int, c varchar) distributed by (a);
insert into test1
	select id, id*2, 'text' || id::varchar
		from generate_series(1,50000000) id;

create table test2 (a int, b int, c numeric, d varchar) distributed by (a);
insert into test2
	select id*2, id*3, id::numeric*random(), 'text' || id::varchar
		from generate_series(1,50000000) id;

/*
-- 1700 MB
select pg_relation_size('public.test1')/1000./1000.;
-- 2500 MB
select ig_relation_size('public.test2')/1000./1000.;

select * from test1 limit 10;
select * from test2 limit 10;
*/


-- Co-located join
explain analyze
select *
	from test1 as t1 inner join test2 as t2 on t1.a = t2.a;

/*
"Gather Motion 72:1  (slice1; segments: 72)  (cost=1176911.82..2503268.42 rows=49999984 width=53)"
"  Rows out:  25000000 rows at destination with 260 ms to first row, 20689 ms to end, start offset by 1.983 ms."
"  ->  Hash Join  (cost=1176911.82..2503268.42 rows=694445 width=53)"
"        Hash Cond: t2.a = t1.a"
"        Rows out:  Avg 347222.2 rows x 72 workers.  Max 347341 rows (seg35) with 298 ms to first row, 717 ms to end, start offset by 5.333 ms."
"        Executor memory:  32542K bytes avg, 32546K bytes max (seg66)."
"        Work_mem used:  32542K bytes avg, 32546K bytes max (seg66). Workfile: (0 spilling, 0 reused)"
"        (seg35)  Hash chain length 1.1 avg, 5 max, using 606458 of 2097211 buckets."
"        ->  Append-only Scan on test2 t2  (cost=0.00..576356.84 rows=694445 width=33)"
"              Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 7.082 ms to first row, 90 ms to end, start offset by 4.550 ms."
"        ->  Hash  (cost=551911.92..551911.92 rows=694445 width=20)"
"              Rows in:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 248 ms to end, start offset by 27 ms."
"              ->  Append-only Scan on test1 t1  (cost=0.00..551911.92 rows=694445 width=20)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 1.417 ms to first row, 68 ms to end, start offset by 27 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 417K bytes."
"  (slice1)    Executor memory: 90524K bytes avg x 72 workers, 90524K bytes max (seg0).  Work_mem: 32546K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 22109.394 ms"
*/

-- Join with single redistribute
explain analyze
select *
	from test1 as t1 inner join test2 as t2 on t1.a = t2.b;

/*
"Gather Motion 72:1  (slice2; segments: 72)  (cost=1176911.82..3503268.10 rows=49999984 width=53)"
"  Rows out:  16666666 rows at destination with 301 ms to first row, 14705 ms to end, start offset by 272 ms."
"  ->  Hash Join  (cost=1176911.82..3503268.10 rows=694445 width=53)"
"        Hash Cond: t2.b = t1.a"
"        Rows out:  Avg 231481.5 rows x 72 workers.  Max 232294 rows (seg64) with 664 ms to first row, 1860 ms to end, start offset by 278 ms."
"        Executor memory:  32542K bytes avg, 32546K bytes max (seg66)."
"        Work_mem used:  32542K bytes avg, 32546K bytes max (seg66). Workfile: (0 spilling, 0 reused)"
"        (seg64)  Hash chain length 1.1 avg, 5 max, using 607428 of 2097211 buckets."
"        ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1576356.52 rows=694445 width=33)"
"              Hash Key: t2.b"
"              Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 695997 rows (seg9) with 0.043 ms to first row, 2417 ms to end, start offset by 676 ms."
"              ->  Append-only Scan on test2 t2  (cost=0.00..576356.84 rows=694445 width=33)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 9.919 ms to first row, 117 ms to end, start offset by 280 ms."
"        ->  Hash  (cost=551911.92..551911.92 rows=694445 width=20)"
"              Rows in:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 332 ms to end, start offset by 304 ms."
"              ->  Append-only Scan on test1 t1  (cost=0.00..551911.92 rows=694445 width=20)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 19 ms to first row, 87 ms to end, start offset by 304 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 423K bytes."
"  (slice1)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice2)    Executor memory: 90445K bytes avg x 72 workers, 90445K bytes max (seg0).  Work_mem: 32546K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 15910.019 ms"
*/

-- Join with 2 redistributions
explain analyze
select *
    from test1 as t1 inner join test2 as t2 on t1.b = t2.b;

/*
"Gather Motion 72:1  (slice3; segments: 72)  (cost=2176911.66..4503267.94 rows=49999984 width=53)"
"  Rows out:  16666666 rows at destination with 1050 ms to first row, 14314 ms to end, start offset by 275 ms."
"  ->  Hash Join  (cost=2176911.66..4503267.94 rows=694445 width=53)"
"        Hash Cond: t2.b = t1.b"
"        Rows out:  Avg 231481.5 rows x 72 workers.  Max 232175 rows (seg33) with 1042 ms to first row, 2124 ms to end, start offset by 283 ms."
"        Executor memory:  32542K bytes avg, 32551K bytes max (seg9)."
"        Work_mem used:  32542K bytes avg, 32551K bytes max (seg9). Workfile: (0 spilling, 0 reused)"
"        (seg33)  Hash chain length 1.1 avg, 5 max, using 604440 of 2097211 buckets."
"        ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1576356.52 rows=694445 width=33)"
"              Hash Key: t2.b"
"              Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 695997 rows (seg9) with 0.042 ms to first row, 2233 ms to end, start offset by 1325 ms."
"              ->  Append-only Scan on test2 t2  (cost=0.00..576356.84 rows=694445 width=33)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 33 ms to first row, 140 ms to end, start offset by 286 ms."
"        ->  Hash  (cost=1551911.76..1551911.76 rows=694445 width=20)"
"              Rows in:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 1014 ms to end, start offset by 311 ms."
"              ->  Redistribute Motion 72:72  (slice2; segments: 72)  (cost=0.00..1551911.76 rows=694445 width=20)"
"                    Hash Key: t1.b"
"                    Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 694638 rows (seg9) with 0.108 ms to first row, 768 ms to end, start offset by 311 ms."
"                    ->  Append-only Scan on test1 t1  (cost=0.00..551911.92 rows=694445 width=20)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 3.396 ms to first row, 102 ms to end, start offset by 286 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 428K bytes."
"  (slice1)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice2)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice3)    Executor memory: 90494K bytes avg x 72 workers, 90494K bytes max (seg0).  Work_mem: 32551K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 15540.258 ms"
*/

-- Double redistribution and join on non-unique field
explain analyze
select *
    from test1 as t1 inner join test2 as t2 on t1.b = t2.c::int;

/*
"Gather Motion 72:1  (slice3; segments: 72)  (cost=2176911.66..4628267.90 rows=49999984 width=53)"
"  Rows out:  25006185 rows at destination with 1067 ms to first row, 19140 ms to end, start offset by 2.279 ms."
"  ->  Hash Join  (cost=2176911.66..4628267.90 rows=694445 width=53)"
"        Hash Cond: t2.c::integer = t1.b"
"        Rows out:  Avg 347308.1 rows x 72 workers.  Max 348523 rows (seg5) with 1064 ms to first row, 3325 ms to end, start offset by 4.126 ms."
"        Executor memory:  32542K bytes avg, 32551K bytes max (seg9)."
"        Work_mem used:  32542K bytes avg, 32551K bytes max (seg9). Workfile: (0 spilling, 0 reused)"
"        (seg5)   Hash chain length 1.1 avg, 5 max, using 606096 of 2097211 buckets."
"        ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1576356.52 rows=694445 width=33)"
"              Hash Key: t2.c::integer"
"              Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 696123 rows (seg4) with 0.060 ms to first row, 2434 ms to end, start offset by 1068 ms."
"              ->  Append-only Scan on test2 t2  (cost=0.00..576356.84 rows=694445 width=33)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 7.178 ms to first row, 113 ms to end, start offset by 14 ms."
"        ->  Hash  (cost=1551911.76..1551911.76 rows=694445 width=20)"
"              Rows in:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 1033 ms to end, start offset by 36 ms."
"              ->  Redistribute Motion 72:72  (slice2; segments: 72)  (cost=0.00..1551911.76 rows=694445 width=20)"
"                    Hash Key: t1.b"
"                    Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 694638 rows (seg9) with 0.070 ms to first row, 774 ms to end, start offset by 36 ms."
"                    ->  Append-only Scan on test1 t1  (cost=0.00..551911.92 rows=694445 width=20)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 2.591 ms to first row, 110 ms to end, start offset by 12 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 428K bytes."
"  (slice1)    Executor memory: 1570K bytes avg x 72 workers, 1570K bytes max (seg0)."
"  (slice2)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice3)    Executor memory: 90558K bytes avg x 72 workers, 90558K bytes max (seg0).  Work_mem: 32551K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 20600.596 ms"
*/

-- Double redistribution and join on text fields
explain analyze
select *
    from test1 as t1 inner join test2 as t2 on t1.c = t2.d;

/*
"Gather Motion 72:1  (slice3; segments: 72)  (cost=2176911.66..4503267.94 rows=49999984 width=53)"
"  Rows out:  50000000 rows at destination with 1362 ms to first row, 41909 ms to end, start offset by 2.254 ms."
"  ->  Hash Join  (cost=2176911.66..4503267.94 rows=694445 width=53)"
"        Hash Cond: t2.d::text = t1.c::text"
"        Rows out:  Avg 694444.4 rows x 72 workers.  Max 696236 rows (seg34) with 1352 ms to first row, 7430 ms to end, start offset by 11 ms."
"        Executor memory:  32542K bytes avg, 32626K bytes max (seg34)."
"        Work_mem used:  32542K bytes avg, 32626K bytes max (seg34). Workfile: (0 spilling, 0 reused)"
"        (seg34)  Hash chain length 1.2 avg, 6 max, using 592147 of 2097211 buckets."
"        ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1576356.52 rows=694445 width=33)"
"              Hash Key: t2.d::text"
"              Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 696236 rows (seg34) with 0.073 ms to first row, 5418 ms to end, start offset by 1363 ms."
"              ->  Append-only Scan on test2 t2  (cost=0.00..576356.84 rows=694445 width=33)"
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 2.553 ms to first row, 119 ms to end, start offset by 9.825 ms."
"        ->  Hash  (cost=1551911.76..1551911.76 rows=694445 width=20)"
"              Rows in:  Avg 694444.4 rows x 72 workers.  Max 696236 rows (seg34) with 1323 ms to end, start offset by 39 ms."
"              ->  Redistribute Motion 72:72  (slice2; segments: 72)  (cost=0.00..1551911.76 rows=694445 width=20)"
"                    Hash Key: t1.c::text"
"                    Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 696236 rows (seg34) with 0.068 ms to first row, 1066 ms to end, start offset by 39 ms."
"                    ->  Append-only Scan on test1 t1  (cost=0.00..551911.92 rows=694445 width=20)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 694541 rows (seg66) with 2.304 ms to first row, 106 ms to end, start offset by 12 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 428K bytes."
"  (slice1)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice2)    Executor memory: 1506K bytes avg x 72 workers, 1506K bytes max (seg0)."
"  (slice3)    Executor memory: 90494K bytes avg x 72 workers, 90494K bytes max (seg0).  Work_mem: 32626K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 44758.736 ms"
*/

-- CPU-intensive workload of MD5 hashing
explain analyze
select md5(a::varchar || '|' || b::varchar || '|' || c::varchar || '|' || d)
	from test2;

/*
"Gather Motion 72:1  (slice1; segments: 72)  (cost=0.00..1826356.44 rows=49999984 width=33)"
"  Rows out:  50000000 rows at destination with 8.821 ms to first row, 23607 ms to end, start offset by 1.378 ms."
"  ->  Append-only Scan on test2  (cost=0.00..1826356.44 rows=694445 width=33)"
"        Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 18 ms to first row, 831 ms to end, start offset by 4.728 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 339K bytes."
"  (slice1)    Executor memory: 367K bytes avg x 72 workers, 367K bytes max (seg0)."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 26384.148 ms"
*/

-- Table sort mainly on 5 segments with 9 involved in total, with spilling
explain analyze
select a, b, c, d, row_number() over (partition by substr(d,1,5) order by c) as rn
	from test2;

/*
"Gather Motion 72:1  (slice2; segments: 72)  (cost=13124546.01..13624545.85 rows=49999984 width=33)"
"  Rows out:  50000000 rows at destination with 147140 ms to first row, 180451 ms to end, start offset by 1.739 ms."
"  ->  Window  (cost=13124546.01..13624545.85 rows=694445 width=33)"
"        Partition By: "?column5?""
"        Order By: c"
"        Rows out:  Avg 5555555.6 rows x 9 workers.  Max 11111111 rows (seg33) with 155453 ms to first row, 181946 ms to end, start offset by 7.298 ms."
"        ->  Sort  (cost=13124546.01..13249545.97 rows=694445 width=33)"
"              Sort Key: "?column5?", c"
"              Rows out:  Avg 5555555.6 rows x 9 workers.  Max 11111111 rows (seg33) with 155453 ms to first row, 173155 ms to end, start offset by 7.303 ms."
"              Executor memory:  34582K bytes avg, 385801K bytes max (seg33)."
"              Work_mem used:  34582K bytes avg, 385801K bytes max (seg33). Workfile: (4 spilling, 0 reused)"
"              Work_mem wanted: 1511780K bytes avg, 1511780K bytes max (seg33) to lessen workfile I/O affecting 4 workers."
"              ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1701356.48 rows=694445 width=33)"
"                    Hash Key: "?column5?""
"                    Rows out:  Avg 5555555.6 rows x 9 workers at destination.  Max 11111111 rows (seg33) with 1.463 ms to first row, 93709 ms to end, start offset by 7.619 ms."
"                    ->  Append-only Scan on test2  (cost=0.00..701356.80 rows=694445 width=33)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 3.331 ms to first row, 326 ms to end, start offset by 8.629 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 346K bytes."
"  (slice1)    Executor memory: 1642K bytes avg x 72 workers, 1642K bytes max (seg0)."
"  (slice2)  * Executor memory: 34866K bytes avg x 72 workers, 386141K bytes max (seg33).  Work_mem: 385801K bytes max, 1511780K bytes wanted."
"Statement statistics:"
"  Memory used: 512000K bytes"
"  Memory wanted: 3023858K bytes"
"Settings:  optimizer=off"
"Total runtime: 187066.419 ms"
*/

-- Sorts and redistributions with all the segments involved
explain analyze
select a, b, c, d,
	max(a) over (partition by (c/100)::int) as v1
	from test2;

/*
"Gather Motion 72:1  (slice2; segments: 72)  (cost=26749091.86..30044404.86 rows=49999984 width=37)"
"  Rows out:  50000000 rows at destination with 1628 ms to first row, 33064 ms to end, start offset by 1.898 ms."
"  ->  Merge Join  (cost=26749091.86..30044404.86 rows=694445 width=37)"
"        Merge Cond: NOT coplan.part_key IS DISTINCT FROM coplan.unnamed_attr_5"
"        Rows out:  Avg 694444.4 rows x 72 workers.  Max 704454 rows (seg5) with 1622 ms to first row, 2847 ms to end, start offset by 21 ms."
"        ->  Subquery Scan coplan  (cost=13374545.93..14897202.47 rows=694445 width=4)"
"              Rows out:  Avg 6874.4 rows x 72 workers.  Max 6915 rows (seg22) with 1642 ms to first row, 1935 ms to end, start offset by 19 ms."
"              ->  GroupAggregate  (cost=13374545.93..14397202.63 rows=694445 width=4)"
"                    Group By: "?column5?""
"                    Rows out:  Avg 6874.4 rows x 72 workers.  Max 6915 rows (seg22) with 1642 ms to first row, 1934 ms to end, start offset by 19 ms."
"                    ->  Shared Scan (share slice:id 2:0)  (cost=13374545.93..13522202.91 rows=694445 width=33)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 704454 rows (seg5) with 1622 ms to first row, 1824 ms to end, start offset by 21 ms."
"                          ->  Sort  (cost=13249545.97..13374545.93 rows=694445 width=33)"
"                                Sort Key: "?column5?""
"                                Rows out:  0 rows (seg0) with 1884 ms to end, start offset by 26 ms."
"                                Executor memory:  81913K bytes avg, 81913K bytes max (seg0)."
"                                Work_mem used:  81913K bytes avg, 81913K bytes max (seg0). Workfile: (0 spilling, 0 reused)"
"                                ->  Redistribute Motion 72:72  (slice1; segments: 72)  (cost=0.00..1826356.44 rows=694445 width=33)"
"                                      Hash Key: "?column5?""
"                                      Rows out:  Avg 694444.4 rows x 72 workers at destination.  Max 704454 rows (seg5) with 3.318 ms to first row, 1223 ms to end, start offset by 21 ms."
"                                      ->  Append-only Scan on test2  (cost=0.00..826356.76 rows=694445 width=33)"
"                                            Rows out:  Avg 694444.4 rows x 72 workers.  Max 694638 rows (seg9) with 9.790 ms to first row, 710 ms to end, start offset by 30 ms."
"        ->  Subquery Scan coplan  (cost=13374545.93..14397202.63 rows=694445 width=33)"
"              Rows out:  Avg 694444.4 rows x 72 workers.  Max 704454 rows (seg5) with 0.022 ms to first row, 485 ms to end, start offset by 1643 ms."
"              ->  Window  (cost=13374545.93..13897202.79 rows=694445 width=33)"
"                    Partition By: "?column5?""
"                    Rows out:  Avg 694444.4 rows x 72 workers.  Max 704454 rows (seg5) with 0.022 ms to first row, 399 ms to end, start offset by 1643 ms."
"                    ->  Shared Scan (share slice:id 2:0)  (cost=13374545.93..13522202.91 rows=694445 width=33)"
"                          Rows out:  Avg 694444.4 rows x 72 workers.  Max 704454 rows (seg5) with 0.002 ms to first row, 72 ms to end, start offset by 1643 ms."
"Slice statistics:"
"  (slice0)    Executor memory: 477K bytes."
"  (slice1)    Executor memory: 1770K bytes avg x 72 workers, 1770K bytes max (seg0)."
"  (slice2)    Executor memory: 82308K bytes avg x 72 workers, 82308K bytes max (seg0).  Work_mem: 81913K bytes max."
"Statement statistics:"
"  Memory used: 512000K bytes"
"Settings:  optimizer=off"
"Total runtime: 39800.156 ms"
*/

create table test3 with (appendonly=true) as
    select t1.a, t1.b, t1.c, t2.b as b2, t2.c as c2, t2.d
        from test1 as t1 inner join test2 as t2 on t1.a = t2.a;
/*
Query returned successfully: 25000000 rows affected, 11217 ms execution time.
Size is 1650 MB
*/
