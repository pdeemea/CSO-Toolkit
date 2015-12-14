/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   16 Apr 2013
 * Description: This module contains a list of functions to put array to str with
 * some specifics to ease code generation
 *
 * Examples of usage:
 * select code_generation.put_to_str (array['a','b','c']);                   -- returns a,b,c
 * select code_generation.put_to_str (array['a','b','c'], 'in_table.');      -- returns in_table.a,in_table.b,in_table.c
 * select code_generation.put_to_str (array['a','b','c'], 'in_table.', '|'); -- returns in_table.a|in_table.b|in_table.c
 */

-- Put array to string with delimiters and prefixes
create or replace function code_generation.put_to_str (in_list varchar[], delimiter varchar, prefix varchar) returns varchar as $BODY$
begin
    return prefix || array_to_string(in_list, delimiter || prefix);
end;
$BODY$
language plpgsql
immutable;

-- Put array to string with prefix and comma as delimiter
create or replace function code_generation.put_to_str (in_list varchar[], prefix varchar) returns varchar as $BODY$
begin
    return code_generation.put_to_str (in_list, ',', prefix);
end;
$BODY$
language plpgsql
immutable;

-- Put array to string with no prefix and comma as delimiter
create or replace function code_generation.put_to_str (in_list varchar[]) returns varchar as $BODY$
begin
    return code_generation.put_to_str (in_list, ',', '');
end;
$BODY$
language plpgsql
immutable;