/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   08 Apr 2013
 * Description: This function allows you to get all a subtree with root in specified node
 * Limitations: Function uses array to store values, so if result set is more than 100k it fails
 * For instance, consider the tree
 *        1      -- level 1
 *       / \    
 *      2   3    -- level 2
 *     / \   \ 
 *    4   5   6  -- level 3
 * For '1' subtree will contain '1','2','3','4','5','6'
 * For '2' subtree will contain '2','4','5'
 * For '3' subtree will contain '3','6'
 * For '4', '5' and '6' subtree will contain only one node
 *
 * Example of usage:
 * -- Create sample data (see graphical sample above)
 * create table public.test_hier (isn int, parentisn int, value varchar, __hier int[], __hier_leaf boolean);
 * insert into  public.test_hier (isn, parentisn, value) values (1, null, 'one'), (2, 1, 'two'),
 *                      (3, 1, 'three'), (4, 2, 'four'), (5, 2, 'five'), (6, 3, 'six');
 * -- Fill hierarchy fields
 * select hierarchies.refresh_hierarchy('public.test_hier', 'isn', 'parentisn', 'int');
 *
 * -- Select the hierarchy structure
 * select * from public.test_hier order by 1;
 * ISN  PARENTISN   VALUE   __HIER      __HIER_LEAF
 * 1    null        one     {1}         false
 * 2    null        two     {1,2}       false
 * 3    null        three   {1,3}       false 
 * 4    null        four    {1,2,4}     true
 * 5    null        five    {1,2,5}     true
 * 6    null        six     {1,3,6}     true
 *
 * -- Get hierarchy level for each node
 * select isn, hierarchies.get_level(__hier) as level from public.test_hier order by 1;
 * ISN  LEVEL
 * 1    1
 * 2    2
 * 3    2
 * 4    3
 * 5    3
 * 6    3
 *
 * -- Get hierarchy level for subtree of node "2"
 * select isn, hierarchies.get_level(__hier,2) as level from public.test_hier where hierarchies.is_subtree(__hier,2) order by 1;
 * ISN  LEVEL
 * 2    1
 * 4    2
 * 5    2
 * Picture of subtree:
 *      2     -- level 1
 *     / \   
 *    4   5   -- level 2
 *
 * -- Get the root id for the subtree of node "2"
 * select isn, hierarchies.root_id(__hier, 2) as root_id from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    null      -- cannot be determined as it is not in subtree of "2"
 * 2    2
 * 3    null      -- cannot be determined as it is not in subtree of "2"
 * 4    2
 * 5    2
 * 6    null      -- cannot be determined as it is not in subtree of "2"
 *
 * -- Get path to root node for the subtree of node "2"
 * select isn, hierarchies.connect_by_path(__hier,' then ',2) as path from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    null      -- cannot be determined as it is not in subtree of "2"
 * 2    2
 * 3    null      -- cannot be determined as it is not in subtree of "2"
 * 4    2 then 4
 * 5    2 then 5
 * 6    null      -- cannot be determined as it is not in subtree of "2"
 *
 * -- Same for all the tree
 * select isn, hierarchies.connect_by_path(__hier,' then ') as path from public.test_hier order by 1;
 * ISN  ROOT_ID
 * 1    1
 * 2    1 then 2
 * 3    1 then 3
 * 4    1 then 2 then 4
 * 5    1 then 2 then 5
 * 6    1 then 3 then 6
 *
 * -- Get 1-up parent
 * select isn, hierarchies.get_parent(__hier, 1) as parent, hierarchies.get_parent(__hier,2) as one_up_parent from public.test_hier order by 1;
 * ISN  PARENT  ONE_UP_PARENT
 * 1    null    null
 * 2    1
 * 3    1
 * 4    2       1
 * 5    2       1
 * 6    3       1
 *
 * -- Get values from parent nodes
 * select t1.value, t2.value as parent_value, t3.value as one_up_parent_value
 *      from public.test_hier as t1
 *          left join public.test_hier as t2 on t2.isn = hierarchies.get_parent(t1.__hier,1)::int
 *          left join public.test_hier as t3 on t3.isn = hierarchies.get_parent(t1.__hier,2)::int
 *      order by t1.isn;
 * VALUE    PARENT_VALUE    ONE_UP_PARENT_VALUE
 * one      null            null
 * two      one             null
 * three    one             null
 * four     two             one
 * five     two             one
 * six      three           one
 *
 * -- Clean up
 * drop table public.test_hier;
 */

/*  Description:
        Function to refresh hierarchy in a table. Table should contain fields __hier
        and __hier_leaf before running this function
    Parameters:
        p_table_name   - full-qualified table name
        p_primary_key  - array of the field names for primary key with their data types
        p_id_field     - name of the ID field
        p_parent_field - name of the PARENT_ID field
        p_id_datatype  - data type of the ID field (character data types are not supported)
        p_no_cycle     - allow cycles in data (value 1) or disallow (value 0). Should be 0 for better performance
        p_index_level  - level of hierarchy where the engine performs reorganize and index (for better performance)
        p_hierarchy_number - hierarchy number, affects the field name for __hier and __hier_leaf fields (__hier, __hier2, __hier3, ...)
*/
create or replace function hierarchies.refresh_hierarchy(p_table_name   varchar,
                                                         p_primary_key  varchar[],
                                                         p_id_field     varchar,
                                                         p_parent_field varchar,
                                                         p_id_datatype  varchar,
                                                         p_no_cycle     int,
                                                         p_index_level  int,
                                                         p_hierarchy_number int) returns void as $BODY$
declare
    p_rowcount      bigint;    -- Number of rows updated on current level
    p_rowcount_prev bigint;    -- Number of rows updated on previous level
    p_level         int;       -- Number of level
    p_pk_fields     varchar[]; -- PK field list
    p_pk_list       varchar;   -- List of primary key fields in temp table
    p_pk_list_dt    varchar;   -- List of primary key fields in temp table with datatypes
    p_pk_join       varchar;   -- Condition to join source table with temp on primary key
    p_no_cycle_condition varchar; -- Condition to filter the cycle case
    i               int;       -- Cycle iterator
    v_hier_field        varchar;
    v_hier_leaf_field   varchar;
begin
    p_pk_list    = '';
    p_pk_list_dt = '';
    p_pk_join = '1 = 1';
    for i in 1..array_upper(p_primary_key,1) loop
        p_pk_fields[i] = substr(p_primary_key[i], 1, position(' ' in p_primary_key[i]) - 1);
        p_pk_list      = p_pk_list    ||  'pk' || i::varchar || ',';
        p_pk_list_dt   = p_pk_list_dt || ',pk' || i::varchar || ' ' || substr(p_primary_key[i], position(' ' in p_primary_key[i]) + 1);
        p_pk_join      = p_pk_join || ' and h.pk' || i::varchar || ' = t.' || p_pk_fields[i];
    end loop;
    if p_hierarchy_number is null or p_hierarchy_number = 1 then
        v_hier_field      = '__hier';
        v_hier_leaf_field = '__hier_leaf';
    else
        v_hier_field      = '__hier' || p_hierarchy_number::varchar;
        v_hier_leaf_field = '__hier_leaf' || p_hierarchy_number::varchar;
    end if;
    -- In case tt_hier table exists (created as permanent) - drop it
    execute 'drop table if exists tt_hier';
    -- Create temp table to store hierarchy information
    execute 'create temporary table tt_hier (
                    id        ' || p_id_datatype || ',
                    parent_id ' || p_id_datatype || ',
                    __hier    ' || p_id_datatype || '[],
                    up_level  smallint' ||
                    p_pk_list_dt || ')
             on commit drop
             distributed by (id)';
    -- Initially fill it
    execute 'insert into tt_hier (' || p_pk_list || ' id, parent_id, __hier, up_level)
                select  ' || array_to_string(p_pk_fields, ',') || ',
                        ' || p_id_field     || ',
                        case when ' || p_parent_field || ' = 0 then null
                             else ' || p_parent_field || '
                        end,
                        case when coalesce(' || p_id_field     || ',0) <> 0 then array[' || p_id_field || ']
                             when coalesce(' || p_parent_field || ',0) <> 0 then array[' || p_parent_field || ']
                             else null::' || p_id_datatype || '[]
                        end,
                        0
                    from ' || p_table_name;
    p_no_cycle_condition = '';
    if p_no_cycle > 0 then
        p_no_cycle_condition = ' and not array[up.parent_id] <@ base.__hier';
    end if;
    -- Update hierarchy path to root node in cycle, level by level
    p_rowcount_prev = 0;
    p_level = 1;
    loop
        raise notice 'Processing level %', p_level;
        -- Main update - gets list of root pretenders, finds their parents and append to the hierarchy array
        
        execute '
            update tt_hier as base
                set __hier   = up.parent_id || __hier,
                    up_level = ' || p_level || '
                from (
                    select main.parent_id,
                           new_parents.id
                        from tt_hier as main
                            inner join (
                                select id
                                    from (
                                        select __hier[1] as id
                                            from  tt_hier
                                            where up_level = ' || p_level || ' - 1
                                        ) as q
                                    where id is not null
                                    group by id
                            ) as new_parents
                            on main.id = new_parents.id
                        where main.parent_id <> new_parents.id
                    ) as up
                where __hier[1] = up.id
                      and base.up_level = ' || p_level || ' - 1 ' ||
                      p_no_cycle_condition;
        GET DIAGNOSTICS p_rowcount = ROW_COUNT;
        raise notice 'Rows updated %', p_rowcount;
        -- If no rows updated - hierarchy id ready
        if p_rowcount = 0 then
            exit;
        end if;
        -- If the same amount of rows updated - we have a cycle in a tree, raise error
        if p_rowcount = p_rowcount_prev and p_no_cycle = 0 then
            raise exception 'Cycle found in the table';
        end if;
        if p_level > 100 then
            raise exception 'Somethisg weird has happened: hierarchy depth increased 100 levels. Check it manually';
        end if;
        
        if p_level = p_index_level then
            execute 'alter table tt_hier set with (reorganize = true)';
            execute 'create index idx_tt_hier on tt_hier using btree(id)';
            set random_page_cost = 1;
        end if;
        p_rowcount_prev = p_rowcount;
        p_level = p_level + 1;
    end loop;
    reset random_page_cost;
    -- Fill the source table with data
    execute 'update ' || p_table_name || ' as t
                set ' || v_hier_field || ' = h.__hier,
                    ' || v_hier_leaf_field || ' = leaf.id is not null
                    from tt_hier as h
                         left join (
                            select h1.id
                                from tt_hier as h1
                                    left join tt_hier as h2
                                    on h1.id = h2.parent_id
                                where h2.id is null
                                group by h1.id
                        ) as leaf
                        on h.id = leaf.id
                    where ' || p_pk_join;
    -- Reorganize source table
    execute 'alter table ' || p_table_name || ' set with (reorganize=true)';
end;
$BODY$
language plpgsql volatile;

create or replace function hierarchies.refresh_hierarchy(p_table_name   varchar,
                                                         p_primary_key  varchar[],
                                                         p_id_field     varchar,
                                                         p_parent_field varchar,
                                                         p_id_datatype  varchar,
                                                         p_no_cycle     int,
                                                         p_index_level  int) returns void as $BODY$
    select hierarchies.refresh_hierarchy($1,$2,$3,$4,$5,$6,$7, null);
$BODY$
language sql volatile;

/*  Description:
        Function to get the node level in specified hierarchy. If called with id=null,
        level is calculated until the root is reached, if not - level is calculated unless
        specified node is found (if not found - null is returned)
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function hierarchies.get_level(__hier numeric[], id numeric) returns int as $BODY$
declare
    i        int;
    res      int;
    is_found boolean;
begin
    if id is null then
        res = array_upper(__hier, 1);
    else
        res      = 1;
        is_found = false;
        i        = array_upper(__hier, 1);
        loop
            if i = 0 then
                exit;
            end if;
            if __hier[i] = id then
                is_found = true;
                exit;
            end if;
            res = res + 1;
            i = i - 1;
        end loop;
        if not is_found then
            res = null;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function hierarchies.get_level(__hier numeric[]) returns int as $BODY$
    select hierarchies.get_level($1, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the root id of the current hierarchy branch. If id=null,
        it returns the root node, if id is not null then if id is in the parent nodes
        for this node it returns id else it returns null
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function hierarchies.root_id(__hier numeric[], id numeric) returns numeric as $BODY$
declare
    res      numeric;
begin
    if id is null then
        res = __hier[1];
    else
        if array[id] <@ __hier then
            res = id;
        else
            res = null;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function hierarchies.root_id(__hier numeric[]) returns numeric as $BODY$
    select hierarchies.root_id($1, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the path (list of IDs) to root node in the branch. If the id is null,
        it returns path to root of the branch, if not - it return path to the specified id (if
        in is in parent nodes for specified hierarchy) or null if not
        This function does not return path
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        dlm     - delimiter to be used to separate 
        id      - value of the root id
*/
create or replace function hierarchies.connect_by_path(__hier numeric[], dlm varchar, id numeric) returns varchar as $BODY$
declare
    i        int;
    res      varchar;
    is_found boolean;
begin
    if id is null then
        res = array_to_string(__hier, dlm);
    else
        res = null;
        if array[id] <@ __hier then
            for i in 1 .. array_upper(__hier, 1) loop
                if __hier[i] = id then
                    res = array_to_string(__hier[i : array_upper(__hier,1)], dlm);
                    exit;
                end if;
            end loop;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function hierarchies.connect_by_path(__hier numeric[], dlm varchar) returns varchar as $BODY$
    select hierarchies.connect_by_path($1, $2, null);
$BODY$
language sql immutable;

create or replace function hierarchies.connect_by_path(__hier numeric[], id numeric) returns varchar as $BODY$
    select hierarchies.connect_by_path($1, ',', $2);
$BODY$
language sql immutable;

create or replace function hierarchies.connect_by_path(__hier numeric[]) returns varchar as $BODY$
    select hierarchies.connect_by_path($1, ',', null);
$BODY$
language sql immutable;

/*  Description:
        Function returns the id of parent node "plevel" levels above the current node.
        If id is specified it is considered as root node and all the nodes above it are
        ignored
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        plevel  - number of levels above current node
        id      - value of the root id
*/
create or replace function hierarchies.get_parent(__hier numeric[], plevel int, id numeric) returns numeric as $BODY$
declare
    i          int;    
    res        numeric;
begin
    res = null;
    if id is null then
        i = array_upper(__hier, 1);
        if plevel+1 <= i then
            res = __hier[i-plevel];
        end if;
    else
        res = null;
        if array[id] <@ __hier then
            i = array_upper(__hier, 1);
            loop
                if (i = 0) or (__hier[i] = id) then
                    exit;
                end if;
                i = i - 1;
            end loop;
            if (i > 0) and array_upper(__hier,1) - i - plevel >= 0 then
                res = __hier[array_upper(__hier,1) - plevel];
            end if;
        end if;
    end if;
    return res;
end;
$BODY$
language plpgsql immutable;

create or replace function hierarchies.get_parent(__hier numeric[], plevel int) returns numeric as $BODY$
    select hierarchies.get_parent($1, $2, null);
$BODY$
language sql immutable;

/*  Description:
        Function returns "true" if the node is in subtree of specified node and "false" if not
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function hierarchies.is_subtree(__hier numeric[], id numeric) returns boolean as $BODY$
    select array[$2] <@ $1;
$BODY$
language sql immutable;

/*  Description:
        Function returns "true" if the node is in subtree of specified node and "false" if not
        For the passed node itself it returs false
    Parameters:
        __hier  - hierarchy array filled by hierarchies.refresh_hierarchy function
        id      - value of the root id
*/
create or replace function hierarchies.is_subtree_strict(__hier numeric[], id numeric) returns boolean as $BODY$
    select array[$2] <@ $1[1:(array_upper($1,1)-1)];
$BODY$
language sql immutable;

/*  Description:
        Function to create custom hierarchy on any join condition. Also allows to calculate
        connect_by_root, connect_by_path, level of hierarchy
    Parameters:
        p_table_name   - full-qualified table name
        p_output_table - full-qualified output table name
        p_primary_key  - array of the field names for primary key with their data types
        p_start_with_table     - table that contains primary keys for the rows hierarchy should start with     varchar,
        p_start_with_condition - condition that is used to extract root rows from master table
        p_connect_by   - statement for joining parent and child row. To specify variable from parent row
                         use <parent>, for child use <child>. For example, "<parent>.id = <child>.parent_id and <child>.value like '%foo%'"
        p_connect_by_root - list of fields which should contain value from root
        p_connect_by_path - list of the pairs (field + delimiter) to generate connect_by_path to root node
        p_index_level  - level of hierarchy where the engine performs reorganize and index (for better performance)
    Output:
        Table p_table_name with additional fields:
            __level            - level number (root level has level = 1)
            __chier_leaf       - true if the node is a leaf in this hierarchy
            __cbr_<field_name> - fields that contain connect_by_root results
            __cbp_<field_name> - fields that contain connect_by_path results
    Example of use:
        create table public.test_hier (isn int, parentisn int, value varchar, __hier int[], __hier_leaf boolean);
        insert into  public.test_hier (isn, parentisn, value) values (1, null, 'one'), (2, 1, 'two'),
                                      (3, 1, 'three'), (4, 2, 'four'), (5, 2, 'five'), (6, 3, 'six');
        create table test2 (isn int);
        insert into test2 values (1);
        select hierarchies.custom_hierarchy('public.test_hier',
                                            'public.test_hier_res',
                                            array['isn'],
                                            'public.test2',
                                            null,
                                            '<parent>.isn = <child>.parentisn and <child>.value like ''%o%''',
                                            array['value varchar'],
                                            array[array['value', '#']],
                                            100);
        select hierarchies.custom_hierarchy('public.test_hier',
                                            'public.test_hier_res',
                                            array['isn'],
                                            null,
                                            'isn = 2',
                                            '<parent>.isn = <child>.parentisn and <child>.value like ''%o%''',
                                            array['value varchar'],
                                            array[array['value', '#']],
                                            100);
*/
create or replace function hierarchies.custom_hierarchy(p_table_name        varchar,
                                                        p_output_table    varchar,
                                                        p_primary_key     varchar[],
                                                        p_start_with_table     varchar,
                                                        p_start_with_condition varchar,
                                                        p_connect_by      varchar,
                                                        p_connect_by_root varchar[],
                                                        p_connect_by_path varchar[][],
                                                        p_index_level     int) returns void as $BODY$
declare
    p_rowcount      bigint;    -- Number of rows updated on current level
    p_rowcount_prev bigint;    -- Number of rows updated on previous level
    p_level         int;       -- Number of level
    i               int;       -- Cycle iterator    
    v_st_join_condition   varchar;
    v_hier_join_condition varchar;
    v_cbr_fields_list varchar; -- Connect by root fields list with datatypes
    v_cbr_fields_init varchar; 
    v_cbr_fields_calc varchar; 
    v_cbp_fields_list varchar; -- Connect by path fields list
    v_cbp_fields_init varchar; -- Connect by path fields initialization
    v_cbp_fields_calc varchar; -- Connect by path fields calculation
    v_table           varchar;
begin
    if (p_start_with_table is null and p_start_with_condition is null) then
        raise exception 'One of the fields p_start_with_table or p_start_with_condition should be filled';
    end if;
    if (p_start_with_table is not null and p_start_with_condition is not null) then
        raise exception 'You must fill exactly one of the following fields: p_start_with_table or p_start_with_condition';
    end if;
    -- Generate code for joins
    v_st_join_condition   = '1 = 1';
    v_hier_join_condition = replace(replace(p_connect_by, '<parent>', 'h'), '<child>', 't');
    for i in 1..array_upper(p_primary_key,1) loop
        v_st_join_condition = v_st_join_condition || ' and t.' || p_primary_key[i] || ' = s.' || p_primary_key[i];
    end loop;
    -- Generate code for connect_by_root
    v_cbr_fields_list = '';
    v_cbr_fields_init = '';
    v_cbr_fields_calc = '';
    for i in 1..array_upper(p_connect_by_root,1) loop
        v_cbr_fields_list = v_cbr_fields_list || ', ' || '__cbr_' || p_connect_by_root[i];
        v_cbr_fields_init = v_cbr_fields_init || ', ' || 't.' || split_part(p_connect_by_root[i], ' ', 1) || ' as ' ||
                            '__cbr_' || split_part(p_connect_by_root[i], ' ', 1);
        v_cbr_fields_calc = v_cbr_fields_calc || ', ' || 'h.__cbr_' || split_part(p_connect_by_root[i], ' ', 1);
    end loop;
    -- Generate code for connect_by_path
    v_cbp_fields_list = '';
    v_cbp_fields_init = '';
    v_cbp_fields_calc = '';
    for i in 1..array_upper(p_connect_by_path,1) loop
        v_cbp_fields_list = v_cbp_fields_list || ', __cbp_' || p_connect_by_path[i][1] || ' varchar';
        v_cbp_fields_init = v_cbp_fields_init || ', coalesce(t.' || p_connect_by_path[i][1] || '::varchar, '''') as __cbp_' || p_connect_by_path[i][1];
        v_cbp_fields_calc = v_cbp_fields_calc || ', h.__cbp_' || p_connect_by_path[i][1] || ' || ''' ||
                            p_connect_by_path[i][2] || ''' || coalesce(t.' || p_connect_by_path[i][1] || '::varchar, '''') as ' ||
                            '__cbr_' || p_connect_by_path[i][1];
    end loop;
    if strpos(p_output_table, '.') > 0 then
        v_table = substr(p_output_table, strpos(p_output_table, '.')+1);
    else 
        v_table = p_output_table;
    end if;
    -- Manage to have empty output table
    if position('.' in p_output_table) > 0 then
        if (select 1 from information_schema.tables where table_schema || '.' || table_name = p_output_table) is null then
            execute 'create table ' || p_output_table || ' (like ' || p_table_name || ', __level int, __chier_leaf bool' || v_cbp_fields_list || v_cbr_fields_list || ');';
        end if;
    end if;
    -- Truncate passed output table
    execute 'truncate ' || p_output_table;
    -- Initially fill it
    if p_start_with_table is not null then
        execute 'insert into ' || p_output_table || '
                    select  t.*,
                            1 as __level,
                            true as __chier_leaf ' ||
                            v_cbp_fields_init || ' ' ||
                            v_cbr_fields_init || '
                        from ' || p_table_name || ' as t
                            inner join ' || p_start_with_table || ' as s
                            on ' || v_st_join_condition;
    end if;
    if p_start_with_condition is not null then
        execute 'insert into ' || p_output_table || '
                    select  *,
                            1 as __level,
                            true as __chier_leaf ' ||
                            v_cbp_fields_init || ' ' ||
                            v_cbr_fields_init || '
                        from ' || p_table_name || ' as t
                        where ' || p_start_with_condition;
    end if;
    -- Update hierarchy path to root node in cycle, level by level
    p_rowcount_prev = 0;
    p_level = 2;
    loop
        raise notice 'Processing level %', p_level;
        if p_level-1 = p_index_level then
            raise notice 'creating index';
            execute 'alter table ' || p_output_table || ' set with (reorganize = true)';
            execute 'create index ' || v_table || '_idx on ' || p_output_table || ' using btree(' || array_to_string(p_primary_key, ',') || ')';
            set random_page_cost = 1;
        end if;
        -- Main update - gets list of root pretenders, finds their parents and append to the hierarchy array
        execute 'insert into ' || p_output_table || '
                    select t.*,
                           ' || p_level::varchar || ' as __level,
                           true as __chier_leaf ' ||
                           v_cbp_fields_calc || ' ' ||
                           v_cbr_fields_calc || '
                        from ' || p_output_table || ' as h
                            inner join ' || p_table_name || ' as t
                            on ' || v_hier_join_condition || '
                            left join ' || p_output_table || ' as s
                            on ' || v_st_join_condition || '
                        where h.__level = ' || (p_level-1)::varchar || '
                            and s.' || p_primary_key[1] || ' is null';
        GET DIAGNOSTICS p_rowcount = ROW_COUNT;
        raise notice 'Rows updated %', p_rowcount;
        -- If no rows updated - hierarchy id ready
        if p_rowcount = 0 then
            exit;
        end if;
        if p_level > 100 then
            raise exception 'Somethisg weird has happened: hierarchy depth increased 100 levels. Check it manually';
        end if;
        p_rowcount_prev = p_rowcount;
        p_level = p_level + 1;
    end loop;
    reset random_page_cost;
    execute 'drop index if exists ' || p_output_table || '_idx';

    -- Set the non-leaf rows as non-leaf in target table
    execute '
        update ' || p_output_table || ' as s
            set __chier_leaf = false
            from (
                select h. ' || array_to_string(p_primary_key, ',h.') || '
                    from ' || p_output_table || ' as h
                        inner join ' || p_output_table || ' as t
                        on ' || v_hier_join_condition || '
                    group by h. ' || array_to_string(p_primary_key, ',h.') || '
                ) as t
            where ' || v_st_join_condition;
    
    -- Reorganize target table
    execute 'alter table ' || p_output_table || ' set with (reorganize=true)';
end;
$BODY$
language plpgsql volatile;


/*  Description:
        Function to refresh hierarchy in a table. Table should contain fields __hier
        and __hier_leaf before running this function
    Parameters:
        p_table_name   - full-qualified table name
        p_primary_key  - array of the field names for primary key
        p_id_field     - name of the ID field
        p_parent_field - name of the PARENT_ID field
        p_id_datatype  - data type of the ID field (character data types are not supported)
        p_no_cycle     - allow cycles in data (value 1) or disallow (value 0). Should be 0 for better performance
        p_index_level_period - period of hierarchy levels when the engine performs reorganize and index (for better performance)
        p_max_levels   - max number of levels allowed in hierarchy
        p_index_level  - level of hierarchy where the engine performs reorganize and index (for better performance)
        p_hierarchy_number - hierarchy number, affects the field name for __hier and __hier_leaf fields (__hier, __hier2, __hier3, ...)
*/
create or replace function hierarchies.refresh_hierarchy_unsafe(p_table_name   varchar,
                                                                p_primary_key  varchar[],
                                                                p_id_field     varchar,
                                                                p_parent_field varchar,
                                                                p_id_datatype  varchar,
                                                                p_no_cycle     int,
                                                                p_index_level_period int,
                                                                p_max_levels   int,
                                                                p_hierarchy_number int) returns void as $BODY$
declare
    p_rowcount      bigint;    -- Number of rows updated on current level
    p_rowcount_prev bigint;    -- Number of rows updated on previous level
    p_level         int;       -- Number of level
    p_pk_fields     varchar[]; -- PK field list
    p_pk_list       varchar;   -- List of primary key fields in temp table
    p_pk_list_dt    varchar;   -- List of primary key fields in temp table with datatypes
    p_pk_join       varchar;   -- Condition to join source table with temp on primary key
    p_no_cycle_condition varchar; -- Condition to filter the cycle case
    i               int;       -- Cycle iterator
    v_index_level   int;
    v_hier_field        varchar;
    v_hier_leaf_field   varchar;
begin
    p_pk_list    = '';
    p_pk_list_dt = '';
    p_pk_join = '1 = 1';
    for i in 1..array_upper(p_primary_key,1) loop
        p_pk_fields[i] = substr(p_primary_key[i], 1, position(' ' in p_primary_key[i]) - 1);
        p_pk_list      = p_pk_list    ||  'pk' || i::varchar || ',';
        p_pk_list_dt   = p_pk_list_dt || ',pk' || i::varchar || ' ' || substr(p_primary_key[i], position(' ' in p_primary_key[i]) + 1);
        p_pk_join      = p_pk_join || ' and h.pk' || i::varchar || ' = t.' || p_pk_fields[i];
    end loop;
    if p_hierarchy_number is null or p_hierarchy_number = 1 then
        v_hier_field      = '__hier';
        v_hier_leaf_field = '__hier_leaf';
    else
        v_hier_field      = '__hier' || p_hierarchy_number::varchar;
        v_hier_leaf_field = '__hier_leaf' || p_hierarchy_number::varchar;
    end if;
    -- In case tt_hier table exists (created as permanent) - drop it
    execute 'drop table if exists tt_hier';
    -- Create temp table to store hierarchy information
    execute 'create temporary table tt_hier (
                    id        ' || p_id_datatype || ',
                    parent_id ' || p_id_datatype || ',
                    __hier    ' || p_id_datatype || '[],
                    up_level  smallint' ||
                    p_pk_list_dt || ')
             on commit drop
             distributed by (id)';
    -- Initially fill it
    execute 'insert into tt_hier (' || p_pk_list || ' id, parent_id, __hier, up_level)
                select  ' || array_to_string(p_pk_fields, ',') || ',
                        ' || p_id_field     || ',
                        case when ' || p_parent_field || ' = 0 then null
                             else ' || p_parent_field || '
                        end,
                        case when coalesce(' || p_id_field     || ',0) <> 0 then array[' || p_id_field || ']
                             when coalesce(' || p_parent_field || ',0) <> 0 then array[' || p_parent_field || ']
                             else null::' || p_id_datatype || '[]
                        end,
                        0
                    from ' || p_table_name;
    p_no_cycle_condition = '';
    if p_no_cycle > 0 then
        p_no_cycle_condition = ' and not array[up.parent_id] <@ base.__hier';
    end if;
    -- Update hierarchy path to root node in cycle, level by level
    p_rowcount_prev = 0;
    p_level = 1;
    v_index_level = p_index_level_period;
    loop
        raise notice 'Processing level %', p_level;
        -- Main update - gets list of root pretenders, finds their parents and append to the hierarchy array
        
        execute '
            update tt_hier as base
                set __hier   = up.parent_id || __hier,
                    up_level = ' || p_level || '
                from (
                    select main.parent_id,
                           new_parents.id
                        from tt_hier as main
                            inner join (
                                select id
                                    from (
                                        select __hier[1] as id
                                            from  tt_hier
                                            where up_level = ' || p_level || ' - 1
                                        ) as q
                                    where id is not null
                                    group by id
                            ) as new_parents
                            on main.id = new_parents.id
                        where main.parent_id <> new_parents.id
                    ) as up
                where __hier[1] = up.id
                      and base.up_level = ' || p_level || ' - 1 ' ||
                      p_no_cycle_condition;
        GET DIAGNOSTICS p_rowcount = ROW_COUNT;
        raise notice 'Rows updated %', p_rowcount;
        -- If no rows updated - hierarchy id ready
        if p_rowcount = 0 then
            exit;
        end if;
        -- If the same amount of rows updated - we have a cycle in a tree, raise error
        if p_rowcount = p_rowcount_prev and p_no_cycle = 0 then
            raise exception 'Cycle found in the table';
        end if;
        if p_level > p_max_levels then
            raise exception 'Somethisg weird has happened: hierarchy depth increased % levels. Check it manually', p_max_levels;
        end if;
        
        if v_index_level <= 0 then
            execute 'drop index if exists idx_tt_hier';
            execute 'alter table tt_hier set with (reorganize = true)';
            execute 'create index idx_tt_hier on tt_hier using btree(id)';
            set random_page_cost = 1;
            v_index_level = p_index_level_period;
        end if;
        p_rowcount_prev = p_rowcount;
        p_level = p_level + 1;
        v_index_level = v_index_level - 1;
    end loop;
    reset random_page_cost;
    -- Fill the source table with data
    execute 'update ' || p_table_name || ' as t
                set ' || v_hier_field || ' = h.__hier,
                    ' || v_hier_leaf_field || ' = leaf.id is not null
                    from tt_hier as h
                         left join (
                            select h1.id
                                from tt_hier as h1
                                    left join tt_hier as h2
                                    on h1.id = h2.parent_id
                                where h2.id is null
                                group by h1.id
                        ) as leaf
                        on h.id = leaf.id
                    where ' || p_pk_join;
    -- Reorganize source table
    execute 'alter table ' || p_table_name || ' set with (reorganize=true)';
end;
$BODY$
language plpgsql volatile;

create or replace function hierarchies.refresh_hierarchy_unsafe(p_table_name   varchar,
                                                                p_primary_key  varchar[],
                                                                p_id_field     varchar,
                                                                p_parent_field varchar,
                                                                p_id_datatype  varchar,
                                                                p_no_cycle     int,
                                                                p_index_level_period int,
                                                                p_max_levels   int) returns void as $BODY$
    select hierarchies.refresh_hierarchy_unsafe($1,$2,$3,$4,$5,$6,$7,$8,null);
$BODY$
language sql volatile;