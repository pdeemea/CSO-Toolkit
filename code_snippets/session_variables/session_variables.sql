/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2014. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   27 Jan 2014
 * Description: This module contains functions to work with session level variables.
 * All the functions are devided in set* and get* functions: set ones are responsible
 * for storing the values for specific variables and get ones are for retrieving them.
 * The last characler in function name refers to data type: v for varchar, d for date,
 * n for numeric and t for timestamp (_pl functions are internal ones).
 * IMPORTANT NOTICE: Functions from this module should be used only inside a single
 * transaction, because after the transaction finishes Greenplum does not guarantee
 * to store segment sessions, which means that value of the _SHARED on segment level
 * might be lost. However, master session is not dropped after the transaction is
 * finished so if you use only master servers get/set it can be used regardless
 * transactions. If you want to continue using the session variables even between
 * transactions, in the beginning of next transaction you should propogate values to
 * the segment sessions by issuing a call to propogate() function. Also you can save
 * the key-value map to table and load from it.
 *
 * Examples of usage:
 * select setparamv('test', 'aaa');
 * select getparamv('test');
 * select setparamd('test', '2013-01-01'::date);
 * select getparamd('test');
 * select setparamn('test', 1.234);
 * select getparamn('test');
 * select setparamt('test', '2013-01-01 11:22:33'::timestamp);
 * select getparamt('test');
 * 
 * create or replace view test_view as
 *     select oid::int, getparamv('test') as val from gp_dist_random('pg_class');

 * create or replace function test () returns void as $BODY$
 * declare v_tmp text;
 * begin
 *     select setparamv('test', 'bla') into v_tmp;
 *     select array_to_string(array_agg(distinct getparamv('test')), ',') from gp_dist_random('pg_class') into v_tmp;
 *     raise notice '%', v_tmp;
 *     select array_to_string(array_agg(distinct val), ',') from test_view into v_tmp;
 *     raise notice '%', v_tmp;
 *     select setparamv('test', 'bla2') into v_tmp;
 *     select array_to_string(array_agg(distinct getparamv('test')), ',') from gp_dist_random('pg_class') into v_tmp;
 *     raise notice '%', v_tmp;
 *     select array_to_string(array_agg(distinct val), ',') from test_view into v_tmp;
 *     raise notice '%', v_tmp;
 *     select setparamv('test', 'bla3') into v_tmp;
 *     select array_to_string(array_agg(distinct getparamv('test')), ',') from gp_dist_random('pg_class') into v_tmp;
 *     raise notice '%', v_tmp;
 *     select array_to_string(array_agg(distinct val), ',') from test_view into v_tmp;
 *     raise notice '%', v_tmp;
 * end;
 * $BODY$ language plpgsql
 * volatile;
 * select test();
 * 
 * select setparamv('test1', 'aaa');
 * select setparamv('test2', 'bbb');
 * select setparamv('test3', 'ccc');
 * create table test_table (a int, b varchar);
 * insert into test_table (a, b) values (1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4');
 * select a, b, getparamv(b) from test_table;
 *  
 * select savetotable('public.test_saved');
 * select * from public.test_saved;
 * select setparamv('test', 'blablabla');
 * select setparamn('test5', 1.12345);
 * select getparamn('test5');
 * select readfromtable('public.test_saved');
 * select getparamn('test');
 * select getparamn('test5');
 */

CREATE or replace FUNCTION helpers.setparamv_pl(name text, val varchar) RETURNS void AS $$
$_SHARED{$_[0]} = $_[1];
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION helpers.setparamv(name text, val varchar) RETURNS void AS $$
begin
perform helpers.setparamv_pl(name, val) from gp_dist_random('pg_class');
perform helpers.setparamv_pl(name, val);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE or replace FUNCTION helpers.setparamn_pl(name text, val numeric) RETURNS void AS $$
$_SHARED{$_[0]} = $_[1];
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION helpers.setparamn(name text, val numeric) RETURNS void AS $$
begin
perform helpers.setparamn_pl(name, val) from gp_dist_random('pg_class');
perform helpers.setparamn_pl(name, val);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE or replace FUNCTION helpers.setparamd_pl(name text, val date) RETURNS void AS $$
$_SHARED{$_[0]} = $_[1];
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION helpers.setparamd(name text, val date) RETURNS void AS $$
begin
perform helpers.setparamd_pl(name, val) from gp_dist_random('pg_class');
perform helpers.setparamd_pl(name, val);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE or replace FUNCTION helpers.setparamt_pl(name text, val timestamp) RETURNS void AS $$
$_SHARED{$_[0]} = $_[1];
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION helpers.setparamt(name text, val timestamp) RETURNS void AS $$
begin
perform helpers.setparamt_pl(name, val) from gp_dist_random('pg_class');
perform helpers.setparamt_pl(name, val);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE or replace FUNCTION helpers.getparamv(name text) RETURNS varchar AS $$
return $_SHARED{$_[0]};
$$ LANGUAGE plperl
immutable;

CREATE or replace FUNCTION helpers.getparamn(name text) RETURNS numeric AS $$
return $_SHARED{$_[0]};
$$ LANGUAGE plperl
immutable;

CREATE or replace FUNCTION helpers.getparamd(name text) RETURNS date AS $$
return $_SHARED{$_[0]};
$$ LANGUAGE plperl
immutable;

CREATE or replace FUNCTION helpers.getparamt(name text) RETURNS timestamp AS $$
return $_SHARED{$_[0]};
$$ LANGUAGE plperl
immutable;

/*  List of stored keys */
create or replace function helpers.getparamlist() returns setof text as $$
foreach my $key (keys %_SHARED) {
    return_next($key);
}
return undef;
$$ language plperl
volatile;

/*  List of stored key-value pairs  */
create or replace function helpers.listentries() returns setof text as $$
foreach my $key (keys %_SHARED) {
    return_next("$key -> $_SHARED{$key}");
}
return undef;
$$ language plperl
volatile;

/*  Propogate values to segments    */
create or replace function helpers.propogate() returns void as $$
declare keyname text;
begin
    for keyname in (select * from helpers.getparamlist()) loop
        raise notice 'Propogating %', keyname;
        perform helpers.setparamv(keyname, helpers.getparamv(keyname));
    end loop;
end;
$$ language plpgsql
volatile;

/*  Save key-value map to the table */
create or replace function helpers.savetotable(tablename varchar) returns void as $$
declare keyname text;
begin
    execute 'drop table if exists ' || tablename;
    execute 'create table ' || tablename || ' (keyname varchar, keyvalue varchar) distributed randomly';
    perform helpers.propogate();
    for keyname in (select * from helpers.getparamlist()) loop
        execute 'insert into ' || tablename || ' values (''' || keyname || ''', ''' || helpers.getparamv(keyname) || ''');';
    end loop;
end;
$$ language plpgsql
volatile;

/*  Read key-value map from the table   */
create or replace function helpers.readfromtable(tablename varchar) returns void as $$
declare v_keynames  text[];
        v_keyvalues text[];        
        v_num_key   int;
begin
    execute '
        select  array_agg(keyname),
                array_agg(keyvalue)
        from ' || tablename
        into v_keynames,
             v_keyvalues;
    for v_num_key in 1 .. array_upper(v_keynames, 1) loop
        perform helpers.setparamv(v_keynames[v_num_key], v_keyvalues[v_num_key]);
    end loop;
end;
$$ language plpgsql
volatile;