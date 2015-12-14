/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   28 Nov 2013
 * Description: This module consists of a function that executes a query in Oracle
 * using cx_Oracle package and also can read access credentails from os.ext_connection
 * Outsourcer table
 *
 * Examples of usage:
 * INSERT INTO os.ext_connection (type, server_name, instance_name, port, database_name, user_name, pass)
 *     VALUES ('oracle', '10.1.3.18', null, 1521, 'olap', 'aislogin', '*******'); -- password is masked with stars
 * select data_connectors.execute_oracle(1,                                 'select 1 from dual');
 * select data_connectors.execute_oracle(666,                               'create table test_remote (a numeric(5,2))');
 * select data_connectors.execute_oracle(1,                                 'insert into test_remote (a) values (1.1)');
 * select data_connectors.execute_oracle('system/changeme@192.168.208.155/xe', 'insert into test_remote (a) values (1.2)');
 * select data_connectors.execute_oracle('system/changeme@192.168.208.155/xe', 'insert into test_remote (a) values (1.3)');
 */

/*
 * Description: Function to execute a query on remote Oracle DBMS
 *              Function does not support returning any data and is used to execute
 *              INSERT/UPDATE/DELETE statements or some DDL
 * Input:
 *      requisites  - connection requisites in the form "username|password|servername"
 *      query       - query that will be executed on remote DBMS
 */
create or replace function data_connectors.execute_oracle(requisites varchar, query varchar) returns varchar as $BODY$
try:
    import cx_Oracle
except Exception as e:
    return 'Cannot locate cx_Oracle package: ' + str(e)
try:
    db = cx_Oracle.connect(requisites)
except Exception as e:
    return 'Cannot connect to Oracle: ' + str(e)
try:
    cursor = db.cursor()
    cursor.execute(query)    
    cursor.close()
    db.commit()
except Exception as e:
    db.rollback()
    return 'Cannot run the query, error: ' + str (e)
db.close()
return 'success'
$BODY$
language plpythonu
volatile;

/*
 * Description: Same function as previous
 * Input:
 *      os_conn_id  - connection id for the Oracle connection in os.ext_connection Outsourcer table
 *      query       - query that will be executed on remote DBMS
 */
create or replace function data_connectors.execute_oracle(os_conn_id int, query varchar) returns varchar as $BODY$
declare
    p_username varchar;
    p_password varchar;
    p_server_name varchar;
    p_database_name varchar;
    res varchar;
begin
    select user_name, pass, server_name, database_name
        into p_username, p_password, p_server_name, p_database_name
        from os.ext_connection
        where id = os_conn_id;
    if p_username is null then
        res = 'Cannot get information from os.ext_connection table for id = ' || os_conn_id::varchar;
    else
        select data_connectors.execute_oracle(p_username || '/' || p_password || '@' || p_server_name || '/' || p_database_name, query)
            into res;
    end if;    
    return res;
end;
$BODY$
language plpgsql
volatile;
