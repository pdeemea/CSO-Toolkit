/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   29 Nov 2013
 * Description: This module contains function to execute code in autonomous transaction,
 * that is not affected by rollback of the parent transaction. Also usign this module is
 * the way to have series of independent commits within one query (one plpgsql function)
 *
 * Examples of usage:
 * select shared_system.autonomous_transaction ('select 1');
 * select shared_system.autonomous_transaction ('select 1', 'gpadmin');
 * select shared_system.autonomous_transaction ('select 1', 'gpadmin', 'changeme');
 * select shared_system.autonomous_transaction ('select 1', 'test', 'gpadmin', 'changeme');
 */

/*
 * Description: Function to execute query in autonomous transaction
 * Input:
 *      query       - query that will be executed in autonomous transaction
 *      dbname      - database name that should host the connection
 *      username    - username that should be used to execute this code
 *      password    - password that should be used to authenticate the user
 * Output:
 *      
 * If some of the parameters is not specified they are read from system environment
 * variables. Also this module can read password from .pgpass file
 */
CREATE OR REPLACE FUNCTION shared_system.autonomous_transaction(query text, dbname text, username text, password text) RETURNS text AS $BODY$
global query, dbname, username, password
import os
import sys
# hack to make gplog work as in C context we dont have argv
sys.argv = ['~']
try:
    from gppylib.db import dbconn
except Exception as e:
    return 'Cannot locate gppylib.db.dbconn package: ' + str(e)
if (username is not None and username.strip() == ''):
    username = None
if (password is not None and password.strip() == ''):
    password = None
if (dbname is not None and dbname.strip() == ''):
    dbname   = None
dburl = dbconn.DbURL(hostname = '',
                     dbname   = dbname,
                     username = username,
                     password = password)
try:
    conn = dbconn.connect(dburl)
except Exception as e:
    return 'Cannot connect to database: ' + str(e)
try:
    curs = dbconn.execSQL(conn, query)
except Exception as e:
    conn.close()
    return 'Cannot execute query: ' + str(e)
try:
    conn.commit()
    curs.close()
    conn.close()
except Exception as e:
    return 'Connection interface error: ' + str(e)
return ''
$BODY$
LANGUAGE plpythonu
volatile;

CREATE OR REPLACE FUNCTION shared_system.autonomous_transaction(query text) RETURNS text AS $BODY$
    select shared_system.autonomous_transaction($1, null, null, null);
$BODY$
LANGUAGE sql
volatile;

CREATE OR REPLACE FUNCTION shared_system.autonomous_transaction(query text, username text) RETURNS text AS $BODY$
    select shared_system.autonomous_transaction($1, null, $2, null);
$BODY$
LANGUAGE sql
volatile;

CREATE OR REPLACE FUNCTION shared_system.autonomous_transaction(query text, username text, password text) RETURNS text AS $BODY$
    select shared_system.autonomous_transaction($1, null, $2, $3);
$BODY$
LANGUAGE sql
volatile;