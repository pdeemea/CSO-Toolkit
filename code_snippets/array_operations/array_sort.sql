/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   08 Apr 2013
 * Description: This module contains a list of functions for in-database
 * manipulations with arrays
 *
 * Examples of usage:
 *   select array_operations.array_sort (array[1,2,3,1,2,3,1,2,3,4])::int[]; --returns [1,1,1,2,2,2,3,3,3,4]::int[]
 */

/*
    Functions to sort array. For instance: [1,2,3,1,2,3,4] is [1,1,2,2,3,3,4]
 */
create or replace function array_operations.array_sort_py (a varchar) returns varchar as $BODY$
return '|'.join(sorted(a.split('|')));
$BODY$
language plpythonu
immutable;

create or replace function array_operations.array_sort (a anyarray) returns varchar[] as $BODY$
begin
if a is null then
	return null::varchar[];
end if;
return string_to_array(array_operations.array_sort_py(
		array_to_string(a, '|')), '|');
end;
$BODY$
language plpgsql
immutable;