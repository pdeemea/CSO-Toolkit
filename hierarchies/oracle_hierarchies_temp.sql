/*  Description:
        Function to create custom hierarchy on any join condition. Also allows to calculate
        connect_by_root, connect_by_path, level of hierarchy
    Parameters:
        p_table_name   - full-qualified table name
        p_output_table - full-qualified output table name
        p_primary_key  - array of the field names for primary key
        p_start_with   - table that contains primary keys for the rows hierarchy should start with
        p_connect_by   - statement for joining parent and child row. To specify variable from parent row
                         use <parent>, for child use <child>. For example, "<parent>.id = <child>.parent_id and <child>.value like '%foo%'"
        p_connect_by_root - list of fields which should contain value from root
        p_connect_by_path - list of the pairs (field + delimiter) to generate connect_by_path to root node
        p_index_level  - level of hierarchy where the engine performs reorganize and index (for better performance)
    Output:
        Table p_table_name with additional fields:
            __level            - level number (root level has level = 1)
            __cbr_<field_name> - fields that contain connect_by_root results
            __cbp_<field_name> - fields that contain connect_by_path results
    Example of use:
        create table public.test_hier (isn int, parentisn int, value varchar, __hier int[], __hier_leaf boolean);
        insert into  public.test_hier (isn, parentisn, value) values (1, null, 'one'), (2, 1, 'two'),
                                      (3, 1, 'three'), (4, 2, 'four'), (5, 2, 'five'), (6, 3, 'six');
        create table test2 (isn int);
        insert into test2 values (1);
        select shared_system.custom_hierarchy('public.test_hier',
                                              'public.test_hier_res',
                                              array['isn'],
                                              'public.test2',
                                              '<parent>.isn = <child>.parentisn and <child>.value like ''%o%''',
                                              array['value varchar'],
                                              array[array['value', '#']],
                                              100);
*/
create or replace function shared_system.custom_hierarchy(p_table_name   varchar,
                                                          p_output_table varchar,
                                                          p_primary_key  varchar[],
                                                          p_start_with   varchar,
                                                          p_connect_by   varchar,
                                                          p_connect_by_root varchar[],
                                                          p_connect_by_path varchar[][],
                                                          p_index_level  int) returns void as $BODY$
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
begin
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
    -- Manage to have empty output table
    if position('.' in p_output_table) > 0 then
        if (select 1 from information_schema.tables where table_schema || '.' || table_name = p_output_table) is null then            
            execute 'create table ' || p_output_table || ' (like ' || p_table_name || ', __level int' || v_cbp_fields_list || v_cbr_fields_list || ');';
        end if;
    end if;
    -- Truncate passed output table
    execute 'truncate ' || p_output_table;
    -- Initially fill it
    execute 'insert into ' || p_output_table || '
                select  t.*,
                        1 as __level,
                        false as __is_leaf ' ||
                        v_cbp_fields_init || ' ' ||
                        v_cbr_fields_init || '
                    from ' || p_table_name || ' as t
                        inner join ' || p_start_with || ' as s
                        on ' || v_st_join_condition;
    -- Update hierarchy path to root node in cycle, level by level
    p_rowcount_prev = 0;
    p_level = 2;
    loop
        raise notice 'Processing level %', p_level;
        if p_level-1 = p_index_level then
            execute 'alter table tt_hier set with (reorganize = true)';
            execute 'create index idx_tt_hier on tt_hier using btree(' || array_to_string(p_primary_key, ',') || ')';
            set random_page_cost = 1;
        end if;
        -- Main update - gets list of root pretenders, finds their parents and append to the hierarchy array
        execute 'insert into ' || p_output_table || '
                    select t.*,
                           ' || p_level::varchar || ' as __level ' ||
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
    -- Reorganize target table
    execute 'alter table ' || p_output_table || ' set with (reorganize=true)';
end;
$BODY$
language plpgsql volatile;