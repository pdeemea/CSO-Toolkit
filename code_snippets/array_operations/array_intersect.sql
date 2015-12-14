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
 *   select array_operations.array_intersect (array[1,2,3], array[2,3,4])::int[]; --returns [2,3]::int[]
 */

/*
    Functions to intersect two arrays. For instance: [1,2,3] and [2,4] is [2]
 */
create or replace function array_operations.array_intersect_py (a varchar, b varchar) returns varchar as $BODY$
return '|'.join(sorted(set(a.split('|')) & set(b.split('|'))));
$BODY$
language plpythonu
immutable;

create or replace function array_operations.array_intersect (a anyarray, b anyarray) returns varchar[] as $BODY$
begin
if a is null or b is null then
	return null::varchar[];
end if;
return string_to_array(array_operations.array_intersect_py(
		array_to_string(a, '|'),
		array_to_string(b, '|')), '|');
end;
$BODY$
language plpgsql
immutable;