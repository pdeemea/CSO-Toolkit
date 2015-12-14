/*
 * Copyright (c) Pivotal Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  AGrishchenko@gopivotal.com
 * Date:   05 Dec 2013
 * Description: This module contains functions to work with JSON objects stored
 * in tables. It covers the simplest case with JSON storing the map (key:value)
 *
 * Examples of usage:
 * select helpers.json_set('', 'email', 'agrishchenko@gopivotal.com');
 * select helpers.json_set('{"ICQ": 224094820, "twitter_id": "0x0FFF", "email": "agrishchenko@gopivotal.com"}', 'email', 'fsidi@gopivotal.com');
 * select helpers.json_get('{"ICQ": 224094820, "twitter_id": "0x0FFF", "email": "agrishchenko@gopivotal.com"}', 'email');
 * select helpers.json_check('{"ICQ": 224094820, "twitter_id": "0x0FFF", "email": "agrishchenko@gopivotal.com"}', 'email', 'fsidi@gopivotal.com');
 * select helpers.json_check('{"ICQ": 224094820, "twitter_id": "0x0FFF", "email": "agrishchenko@gopivotal.com"}', 'email', 'agrishchenko@gopivotal.com');
 */

/*
 * Description: Function to set <value> to the <name> in key:value map
 * Input:
 *      json_field  - JSON object passed to the function as text
 *      name        - key for which you want to set <value>
 *      value       - value that you want to be set
 * Output:
 *      New JSON object
 */
create or replace function helpers.json_set(json_field varchar, name varchar, value varchar) returns varchar as $BODY$
import json
if json_field is None or json_field == '':
	j = {}
else:
	j = json.loads(json_field)
j[name] = value
return json.dumps(j)
$BODY$
language plpythonu
volatile;

/*
 * Description: Function to get <value> of the <name> key in key:value map
 * Input:
 *      json_field  - JSON object passed to the function as text
 *      name        - key for which you want to get value
 * Output:
 *      value for specific field and null if field is not found
 */
create or replace function helpers.json_get(json_field varchar, name varchar) returns varchar as $BODY$
import json
if json_field is None or json_field == '':
	j = {}
else:
	j = json.loads(json_field)
res = None
if name in j:
	res = j[name]
return res
$BODY$
language plpythonu
volatile;

/*
 * Description: Function to test whether the value of the <name> key in key:value map
                is equal to the <value> passed to the function
 * Input:
 *      json_field  - JSON object passed to the function as text
 *      name        - key for which you want to test the value
 *      value       - value that you are expecting to get
 * Output:
 *      true if value is equal and false if the value is not equal or not found
 */
create or replace function helpers.json_check(json_field varchar, name varchar, value varchar) returns varchar as $BODY$
import json
if json_field is None or json_field == '':
	j = {}
else:
	j = json.loads(json_field)
res = None
if name in j:
	res = j[name]
return (res == value)
$BODY$
language plpythonu
volatile;