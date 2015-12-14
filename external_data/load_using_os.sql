/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   16 Apr 2013
 * Description: This module consists of a function that loads data from external database
 * (Oracle or MS) using Outsourcer library function os.fn_create_ext_table
 *
 * Examples of usage:
 * INSERT INTO os.ext_connection (type, server_name, instance_name, port, database_name, user_name, pass)
 *     VALUES ('oracle', '10.1.3.18', null, 1521, 'olap', 'aislogin', '*******'); -- password is masked with stars
 * select helpers.load_from_ora ('test.histlog',    -- Target table of data load (this table is dropped and recreated)
                                 'hist.histlog',    -- Table that has a correct schema
                                 'ext.ext_histlog', -- External table that will contain outsourcer call (dropped and recreated if exists)
                                 2,                 -- ID of data source in outsourcer config table os.ext_connection
                                 'select * from histlog where Isn >= gethistlogISn(trunc(sysdate))
                                                and Isn <= gethistlogISn(trunc(sysdate)) + 10000'); -- this query is executed in remote DB
 */
 
/*
 * Input:
 *     target_table   - table that will contain result dataset
 *     like_table     - table that has a correct DDL. Target table will be created with the same DDL
 *     ext_table      - external table that contans call to outsourcer jar-file on GP master
 *     connection_id  - ID of the connection in os.ext_connection
 *     query          - query that will be executed on remote DBMS
 */
create or replace function data_connectors.load_using_os (
        target_table varchar,
        like_table varchar,
        ext_table varchar,
        connection_id int,
        query varchar) returns void as $BODY$
declare
    fld_format varchar[];
    fld_list varchar(10000);
    fn_res varchar(1);
begin
    --List of columns from like_table with data types as array
	select array_agg(q.desc)
        into fld_format
        from (
            select  case
                        when column_name like '%#%' then '"' || column_name || '"'
                        else column_name
                    end
                    || ' ' ||
                    case
                        when data_type in ('character varying', 'character') then data_type || '(' || character_maximum_length::varchar(5) || ')'
                        when data_type in ('numeric') then
                            case
                                when numeric_precision is null or numeric_scale is null then data_type
                                else data_type || '(' || numeric_precision::varchar(2) || ',' || numeric_scale::varchar(2) || ')'
                            end
                        else data_type
                    end as desc
                from information_schema.columns
                where table_schema || '.' || table_name = lower(like_table)
                order by ordinal_position
            ) as q;
    raise notice 'Field format array: %', fld_format;

    --List of columns from like_table as a string
	select array_to_string(a,',')
        into fld_list
        from (
            select array_agg(q.column_name) as a
            from (
                select case
                           when column_name like '%#%' then '"' || column_name || '"'
                           else column_name
                       end
                    from information_schema.columns
                    where table_schema || '.' || table_name = lower(like_table)
                    order by ordinal_position
            ) as q
        ) as q2;
    raise notice 'Field list: %', fld_list;

    --Create external table to read data from Oracle
	select os.fn_create_ext_table(ext_table, fld_format, connection_id, query)
	into fn_res;
		
    --Drop target_table if exists
	execute 'drop table if exists ' || target_table || ';';
		 
    --Create new target_table like like_table
	execute 'create table ' || target_table || ' (like ' || like_table || ');';

    --Load data into target_table
	execute 'insert into ' || target_table || '(' || fld_list || ')'
		|| 'select ' || fld_list || ' from ' || ext_table || ';';

    --Drop external table that was created for data load
	execute 'drop external table if exists ' || ext_table || ';';
end;
$BODY$
language plpgsql
volatile;