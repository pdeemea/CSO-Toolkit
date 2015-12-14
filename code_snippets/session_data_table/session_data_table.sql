/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2014. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   11 Feb 2014
 * Description: This module contains functions to create and maintain the table
 * with the data visible only in current session. This is organized by creating
 * a table with additional __session_id field, creating a view that filters table
 * rows according to the session id and function that cleans up the data. Also
 * this module creates a rule over the view the way you can insert data to the
 * view instead of the table
 *
 * Examples of usage:
 * -- Create session data table
 * select session_data_table_create('public', 'tt_rowid', array['isn numeric', 'foo int'], null);
 * select * from tt_rowid;
 * -- Insert values into it
 * insert into tt_rowid (isn) values(1),(2),(3);
 * select * from tt_rowid;
 * select * from tt_rowid_t;
 * -- Insert values directly to the table under view
 * insert into tt_rowid_t (isn) values(4),(5),(6);
 * select * from tt_rowid;
 * select * from tt_rowid_t;
 * -- Cleanup data for old sessions with reorganize
 * select session_data_table_cleanup('public', 'tt_rowid', true);
 * select * from tt_rowid;
 * select * from tt_rowid_t;
 */

/*
 * Description: Function to create table for session-level data
 * Input:
 *      pSchema         - schema where the table is to be located
 *      pTable          - table name
 *      pColumns        - columns definition array in a format "field_name data_type"
 *      pDistribution   - distribution fields list
 */
create or replace function helpers.session_data_table_create (pSchema varchar, pTable varchar, pColumns varchar[], pDistribution varchar) returns void as $BODY$
declare
    vDistribution   varchar;
    vColumnNames    varchar[];
    vColumnNamesNew varchar[];
    i int;
begin
    for i in 1..array_upper(pColumns, 1) loop
        vColumnNames[i] = split_part(pColumns[i], ' ', 1);
        vColumnNamesNew[i] = 'NEW.' || split_part(pColumns[i], ' ', 1);
    end loop;
    vDistribution = '';
    if pDistribution is not null then
        vDistribution = ' distributed by (' || pDistribution || ')';
    end if;
    execute 'drop table if exists ' || pSchema || '.' || pTable || '_t cascade;';
    execute 'create table ' || pSchema || '.' || pTable || '_t (' ||
                array_to_string(pColumns,',') || ', __session_id int) ' || vDistribution;
    execute 'create or replace view ' || pSchema || '.' || pTable || ' as
                select ' || array_to_string(vColumnNames,',') || ' from ' ||
                pSchema || '.' || pTable || '_t where __session_id = current_setting(''gp_session_id'')::int;';
    execute 'create rule ' || pTable || '_rule AS ON INSERT TO ' ||
                pSchema || '.' || pTable || ' DO INSTEAD INSERT INTO ' ||
                pSchema || '.' || pTable || '_t values (' ||
                array_to_string(vColumnNamesNew,',') || ', current_setting(''gp_session_id'')::int);';
end;
$BODY$
language plpgsql;

/*
 * Description: Function to clean up table for session-level data for current session
 * Input:
 *      pSchema         - schema name
 *      pTable          - table name
 */
create or replace function helpers.session_data_table_cleanup (pSchema varchar, pTable varchar) returns void as $BODY$
begin
    execute 'delete from ' || pSchema || '.' || pTable || '_t
                where __session_id = current_setting(''gp_session_id'')::int;';
end;
$BODY$
language plpgsql;

/*
 * Description: Function to clean up table for session-level data for all the
                sessions that ended/failed without removing data from temp table
 * Input:
 *      pSchema         - schema name
 *      pTable          - table name
 *      pReorganize     - if set to true table is reorganized (requires access exclusive lock - block reads)
 */
create or replace function helpers.session_data_table_full_cleanup (pSchema varchar, pTable varchar, pReorganize bool) returns void as $BODY$
declare
    vSessions int[];
begin
    execute 'select array_agg(sess_id) from pg_stat_activity' into vSessions;
    execute 'delete from ' || pSchema || '.' || pTable || '_t
                where __session_id is null or
                      __session_id not in (' || array_to_string(vSessions, ',') || ');';
    if pReorganize then
        execute 'alter table ' || pSchema || '.' || pTable || '_t set with (reorganize = true);';
    end if;
end;
$BODY$
language plpgsql;

/*
 * Alternative implementation with trigger. It is slower, so I leave it only for reference

    execute 'CREATE TRIGGER ' || pTable || '_trigger BEFORE INSERT ON ' ||
        pSchema || '.' || pTable || '_t FOR EACH ROW EXECUTE PROCEDURE session_data_table_trigger();';
create or replace function session_data_table_trigger () returns trigger as $BODY$
begin
    NEW.__session_id = current_setting('gp_session_id');
    return NEW;
end;
$BODY$
language plpgsql;
*/