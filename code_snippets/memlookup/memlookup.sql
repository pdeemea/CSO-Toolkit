/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2014. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   27 Jan 2014
 * Description: This library contains a set of functions to handle in-memory lookup.
 * This allows you to load a table into memory and then lookup it from the segments.
 * We don't recommend using this function with tables of more than 100'000 rows as it
 * might be slow. To utilize the function, you should first load it into memoty and
 * then query, preferrably in the same transaction.
 *
 * Examples of usage:
 * -- Create sample dictionary
 * create table test (a int, b int, c int);
 * insert into test select id,id*2,id*3 from generate_series(1,10000) id;
 * -- Load it into memory
 * select memlookup_load('public.test', array['a'], array['b', 'c']);
 * -- Create table to lookup it from segment servers
 * create table test2 (a int);
 * insert into test2 select * from generate_series(1,10000);
 * -- Lookup into dictionary from segments
 * select a, memlookup_find('public.test', array[a::varchar], 'c') as c from test2 limit 100;
 * -- Lookup all the values and find maximal int
 * select max(memlookup_find('public.test', array[a::varchar], 'c')::int) from test2;
 * Limitations:
 *   - Functions convert all the table values to text
 *   - Keys provided to the function should be unique for each row of the dictionary
 *   - Text values should not contain pipe '|' and comma ',' values
 *   - Lookup function returns only varchars, you should convert them to needed type manually
 *
 * Main functions description:
 *   memlookup_load - Function to load dictionary into memory:
 *      Input:
 *          p_table   - name of the dictionary table
 *          p_keys    - array of the primary key field names
 *          p_fields  - array of the names for the fields that should be stored in memory
 *   memlookup_find - Function to find the value in memory dictionary based on provided key
 *      Input:
 *          p_table   - name of the dictionary table
 *          p_keys    - array of the values of the keys in dictionary
 *          p_field   - name of the field to return
 */
CREATE OR REPLACE FUNCTION memlookup_load_perl(p_table varchar, p_keys varchar[], p_fields varchar[]) RETURNS void AS $$
    my $p_table  = $_[0];
    my @p_keys   = split(/,/, substr $_[1], 1, -1);
    my @p_fields = split(/,/, substr $_[2], 1, -1);
    my $p_keys_n   = scalar @p_keys;
    my $p_fields_n = scalar @p_fields;
    my $rv = spi_exec_query('select ' . join(',', @p_keys) . ',' . join(',', @p_fields) . " from $p_table;");
    $_SHARED{$p_table . '_schema'} = { keys => join('|',@p_keys), fields => join('|',@p_fields)};
    $_SHARED{$p_table} = {};
    foreach my $rn (0 .. $rv->{processed} - 1) {
        my $key = '';
        foreach my $kn (0 .. $p_keys_n-1) {
            $key = $key . $rv->{rows}[$rn]->{@p_keys[$kn]} . '|';
        }
        my $value = '';
        foreach my $vn (0 .. $p_fields_n-1) {
            $value = $value . $rv->{rows}[$rn]->{@p_fields[$vn]} . '|';
        }
        $_SHARED{$p_table}{$key} = $value;
    }
    return;
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION memlookup_find(p_table varchar, p_keys varchar[], p_field varchar) RETURNS varchar AS $$
    my $p_table  = $_[0];
    my $p_keys   = join('|', split(/,/, substr $_[1], 1, -1)) . '|';
    my @p_fields = split(/\|/, $_SHARED{$p_table . '_schema'}{fields});
    my $p_index = -1;
    my $test = $_SHARED{$p_table . '_schema'}{fields};
    foreach my $i (0 .. scalar @p_fields-1) {
        if (@p_fields[$i] eq $_[2]) {
            $p_index = $i;
        }
    }
    if ($p_index == -1) {
        elog (ERROR, "Cannot find the field $_[2] in memory array");
    }
    my $res = $_SHARED{$p_table}{$p_keys};
    if ($res ne '') {
        $res = (split(/\|/, $res))[$p_index];
    }
    return $res;
$$ LANGUAGE plperl;

CREATE or replace FUNCTION memlookup_setschema_p(p_table varchar, p_keys varchar, p_fields varchar) RETURNS void AS $$
    $_SHARED{$_[0] . '_schema'} = {keys => $_[1], fields => $_[2]};
    return;
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION memlookup_setschema(p_table varchar, p_keys varchar, p_fields varchar) RETURNS void AS $$
begin
    perform memlookup_setschema_p(p_table, p_keys, p_fields) from 
		(select gp_segment_id from gp_dist_random('pg_class') group by gp_segment_id) as q;
    perform memlookup_setschema_p(p_table, p_keys, p_fields);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE or replace FUNCTION memlookup_setdata_p(p_table varchar, p_keys varchar[], p_values varchar[]) RETURNS void AS $$
    if (!exists $_SHARED{$_[0]}) {
        $_SHARED{$_[0]} = {};
    }
    my @keys   = split(/,/, substr $_[1], 1, -1);
    my @values = split(/,/, substr $_[2], 1, -1);
    foreach my $i (0 .. scalar @keys) {
        $_SHARED{$_[0]}{@keys[$i]} = @values[$i];
    }
    return;
$$ LANGUAGE plperl
volatile;

CREATE or replace FUNCTION memlookup_setdata(p_table varchar, p_keys varchar[], p_values varchar[]) RETURNS void AS $$
begin
    perform memlookup_setdata_p(p_table, p_keys, p_values) from
		(select gp_segment_id from gp_dist_random('pg_class') group by gp_segment_id) as q;
    perform memlookup_setdata_p(p_table, p_keys, p_values);
end;
$$ LANGUAGE plpgsql
volatile;

CREATE OR REPLACE FUNCTION memlookup_getkeys(p_table varchar) RETURNS varchar AS $$
    return $_SHARED{$_[0] . '_schema'}{keys};
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION memlookup_getfields(p_table varchar) RETURNS varchar AS $$
    return $_SHARED{$_[0] . '_schema'}{fields};
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION memlookup_getdata(p_table varchar) RETURNS setof varchar[] AS $$
    foreach my $key (keys %{$_SHARED{$_[0]}}) {
        return_next( [$key, $_SHARED{$_[0]}{$key}] );
    }
    return undef;
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION memlookup_load(p_table varchar, p_keys varchar[], p_fields varchar[]) RETURNS void AS $$
declare
    v_keys   varchar[];
    v_values varchar[];
begin
    perform memlookup_load_perl(p_table, p_keys, p_fields);
    perform memlookup_setschema(p_table, memlookup_getkeys(p_table), memlookup_getfields(p_table));
    select  array_agg(d[1]),
            array_agg(d[2])
        into v_keys,
             v_values
        from memlookup_getdata(p_table) as d;
    perform memlookup_setdata(p_table, v_keys, v_values);
end;
$$ LANGUAGE plpgsql;