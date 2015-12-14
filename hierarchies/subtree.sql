/*
 * Copyright (c) EMC Inc, Greenplum division, 2013. All Rights Reserved. 
 *
 * Author: A.Grishchenko
 * Email:  Aleksey.Grishchenko@emc.com
 * Date:   08 Apr 2013
 * Description: This function allows you to get all a subtree with root in specified node
 * Limitations: Function uses array to store values, so if result set is more than 100k it fails
 * For instance, consider the tree
 *        1
 *       / \
 *      2   3
 *     / \   \ 
 *    4   5   6
 * For '1' subtree will contain '1','2','3','4','5','6'
 * For '2' subtree will contain '2','4','5'
 * For '3' subtree will contain '3','6'
 * For '4', '5' and '6' subtree will contain only one node
 *
 * Examples of usage:
 * create table public.test   (id int, parent_id int);
 * insert into public.test    (id, parent_id) values (1, null), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3);
 * select hierarchies.subtree ('public.test', 'id', 'parent_id', 1); --returns [1,2,3,4,5,6]::numeric[]
 * select hierarchies.subtree ('public.test', 'id', 'parent_id', 2); --returns [2,4,5]::numeric[]
 * select hierarchies.subtree ('public.test', 'id', 'parent_id', 3); --returns [3,6]::numeric[]
 * select hierarchies.subtree ('public.test', 'id', 'parent_id', 4); --returns [4]::numeric[]
 * drop table public.test;
 */

/*
    inTable: full-qualified name of input table
    inField: id field of the record
    inParentField: id of the parent record
    inValue: id of the start node
 */    
create or replace function hierarchies.subtree (inTable varchar(128), inField varchar(128), inParentField varchar(128), inValue NUMERIC) returns NUMERIC[] as $BODY$
declare
    curArray NUMERIC[];
    curSize  bigint;
    newArray NUMERIC[];
    newSize  bigint;
    iternum  int;
begin
    -- Initialize array with initial value
    curArray := ARRAY[inValue]::NUMERIC[];
    curSize  := array_upper(curArray,1);
    
    -- New array in empty. It has non-empty size because of cycle condition
    newArray := ARRAY[inValue]::NUMERIC[];
    newSize  := 2;
    
    -- While array size changed after iteration
    while (newSize - curSize > 0) loop
        
        -- Current size of array
        curSize := array_upper(curArray,1);
        
        -- Select child elements for newArray
        execute 'select array_agg( ' || inField || ')
                    from (
                        select ' || inField || '
                            from ' || inTable || ' as t
                                inner join (select unnest(string_to_array(''' || array_to_string(newArray, ',') || ''', '','')::NUMERIC[]) as _val) as q
                                on ' || inParentField || ' = q._val
                            group by ' || inField || ') as q'                                
            into newArray;
        
        -- Remove from new array all the elements that was already tested
        newArray := array_operations.array_minus (newArray, curArray)::NUMERIC[];

        -- Put it as notice
        raise notice 'New nodes: %', newArray;

        -- Add new elements to curArray
        curArray := array_operations.array_merge (curArray, newArray)::NUMERIC[];
            
        -- Output current array
        raise notice 'Total list: %', curArray;
        
        -- New size of array: after adding new elements
        newSize := array_upper(curArray,1);
        
        -- If array size more than 100000 - raise exception
        if (newSize > 100000) then
            raise exception 'ERROR: Target array size is more than 100000 elements (%)! Cannot handle it in memory', curSize + newSize;
        end if;

    end loop;
	
    return curArray;
end;
$BODY$
language plpgsql
volatile;