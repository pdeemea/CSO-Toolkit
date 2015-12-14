/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   16 Apr 2013
 * Description: This module consists of a function that generate field list
 * for selected table as array of varchars
 *
 * Examples of usage:
 * create table public.test (a int, b int);
 * select code_generation.get_field_list ('public.test'); --returns array['a','b']
 */
 
-- Get table field list as array
create or replace function code_generation.get_field_list (table_name varchar(128)) returns varchar[] as $BODY$
declare
    field_list varchar[];
begin
    select array_agg(q.column_name)
        into field_list
        from (
            select case
                       when c.column_name like '%#%' then '"' || c.column_name || '"'
                       else c.column_name
                   end as column_name
                from information_schema.columns as c
                where c.table_schema || '.' || c.table_name = lower(table_name)
                order by ordinal_position
        ) as q;
    return field_list;
end;
$BODY$
language plpgsql
volatile;