/*  Modified GPText function for index creation that allows you to create an index
    for a subset of columns of the table    */
CREATE OR REPLACE FUNCTION gptext.create_index(schema_name text, table_name text, id_col_name text, def_search_col_name text, p_fields varchar[], p_types varchar[])
  RETURNS boolean AS
$BODY$
DECLARE
    in_failure BOOLEAN;
    core_exists	BOOLEAN;
    valid_fqname BOOLEAN;
    success BOOLEAN;
    is_id BOOLEAN;
    is_default_search_col BOOLEAN;
    att RECORD;
    db_name TEXT;
    schema_table_name TEXT;
    fq_name TEXT;
    v_att_id int;
BEGIN
    -- Lock history table to synchronize create/drop
    LOCK TABLE gptext.admin_history IN ACCESS EXCLUSIVE MODE;

    SELECT current_database() INTO db_name;
    schema_table_name := schema_name || '.' || table_name;
    fq_name := db_name || '.' || schema_table_name; 

    -- Check that we are not in failure
    SELECT 
    	count(*) <> 0 INTO in_failure 
    FROM 
    	gptext.status() 
    WHERE 
    	status = 'd';

    IF in_failure = 't' THEN
        RAISE NOTICE 'Cannot create index while one or more text search instances are down.';
        RETURN 'f';
    END IF;
    
    -- Check that supplied db.schema.table exists
    SELECT 
    	count(*) <> 0 INTO valid_fqname 
    FROM 
    	pg_tables 
    WHERE 
    	schemaname = schema_name AND tablename = table_name;
    	
    IF valid_fqname = 'f' THEN
        RAISE NOTICE '% does not exist in this database', schema_table_name;
        RETURN 'f';
    END IF;
    
    -- Check Solr index does not already exist
    SELECT 
    	count(*) > 0 INTO core_exists 
    FROM 
    	gptext.index_statistics(fq_name);
    	
    IF core_exists = 't' THEN
        RAISE NOTICE 'Index for table % already exists.', schema_table_name;
        RETURN 'f';
    END IF;

    -- Validate the id column
    SELECT 
    	COUNT(*) = 1 INTO success 
    FROM 
    	pg_catalog.pg_attribute a, pg_class c, pg_namespace n 
	WHERE 
		a.attrelid = c.oid AND
        c.relname=table_name AND 
		n.oid = c.relnamespace AND 
		n.nspname = schema_name AND 
		a.attnum > 0 AND 
		a.attname = id_col_name AND 
		NOT a.attisdropped AND
		pg_catalog.format_type(a.atttypid, a.atttypmod) in ('bigint', 'int8');

	IF success <> 't' THEN
		RAISE NOTICE 'Column % either does not exist or is not of type BIGINT or INT8', id_col_name;
		return 'f';
	END IF;

	-- Validate the default search column
    SELECT 
    	COUNT(*) = 1 INTO success 
    FROM 
    	pg_catalog.pg_attribute a, pg_class c, pg_namespace n 
	WHERE 
		a.attrelid = c.oid AND
		c.relname=table_name AND
		n.oid = c.relnamespace AND
		n.nspname = schema_name AND
		a.attnum > 0 AND
		a.attname = def_search_col_name AND
		NOT a.attisdropped;

    IF success <> 't' THEN
        RAISE NOTICE 'Default search column % does not exist', def_search_col_name;
        return 'f';
    END IF;
    
    -- Send clone request
    SELECT 
    	count(*) = 0 INTO success 
    FROM 
    	gptext.__clone_template_conf_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name) c 
    WHERE 
    	c = 'f';
    	
	IF success <> 't' THEN
		SELECT count(*) = 0 INTO success FROM gptext.__forced_clean_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name) fc WHERE fc = 'f';
		RAISE EXCEPTION 'Create index operation failed.';
	END IF;
    
    -- Configure the schema.xml file
    For v_att_id IN 1 .. array_upper(p_fields, 1) loop
    
        IF p_fields[v_att_id] = id_col_name THEN
            is_id := 't';
        ELSE
            is_id := 'f';
        END IF;
        IF p_fields[v_att_id] = def_search_col_name THEN
            is_default_search_col := 't';
        ELSE
            is_default_search_col := 'f';
        END IF;
        
        SELECT 
        	count(*) = 0 INTO success 
        FROM 
        	gptext.__add_field_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name, p_fields[v_att_id], p_types[v_att_id], 't', 'f', is_id, is_default_search_col) a 
        WHERE 
        	a = 'f';
        	
		IF success <> 't' THEN
			SELECT count(*) = 0 INTO success FROM gptext.__forced_clean_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name) fc WHERE fc = 'f';
			RAISE EXCEPTION 'Create index operation failed.';
		END IF;

    END LOOP;
        
    -- Send create request which will activate the index
    SELECT 
    	count(*) = 0 INTO success 
    FROM 
    	gptext.__create_index_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name) c 
    WHERE 
    	c = 'f';

	IF success <> 't' THEN
		SELECT count(*) = 0 INTO success FROM gptext.__forced_clean_impl(TABLE(SELECT 1 SCATTER BY 1), fq_name) fc WHERE fc = 'f';
		RAISE EXCEPTION 'Create index operation failed.';
	END IF;	

    INSERT INTO gptext.admin_history (action) VALUES ('Created index ' || fq_name);

    -- Let user know what tables where created
    RAISE INFO 'Created index %', fq_name;
    RETURN success;
END
$BODY$
LANGUAGE plpgsql
VOLATILE;