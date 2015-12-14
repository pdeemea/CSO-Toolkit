--set allow_system_table_mods="DML";
CREATE OR REPLACE FUNCTION utils.clonestat(NAME,NAME,NAME,NAME) RETURNS VOID AS $$
DECLARE
  src_schema NAME;
  src_table NAME;
  trg_schema NAME;
  trg_table NAME;
 
  src_column NAME;
 
  trg_reloid OID;
  trg_attnum INTEGER;
 
  src_reloid OID;
  src_relpages INTEGER;
  src_reltuples FLOAT;
BEGIN
              src_schema := $1;
              src_table := $2;
              trg_schema := $3;
              trg_table := $4;
 
           RAISE INFO 'Cloning source table statistics %.% to target table %.%',src_schema,src_table,trg_schema,trg_table;
 
              -- Acquire the basic table properties from the source table
        SELECT class.oid,class.relpages,class.reltuples
          INTO src_reloid,src_relpages,src_reltuples
          FROM pg_class class
             , pg_namespace nsp
         WHERE nsp.oid = class.relnamespace
           AND nsp.nspname = src_schema
           AND class.relname = src_table
                 ;
 
              -- Acquire the basic table properties from the target table
        SELECT class.oid
          INTO trg_reloid
          FROM pg_class class
             , pg_namespace nsp
         WHERE nsp.oid = class.relnamespace
           AND nsp.nspname = trg_schema
           AND class.relname = trg_table
                 ;
 
              -- Insert tuple count and page count from source table into target
              UPDATE pg_class SET relpages=src_relpages, reltuples=src_reltuples
         WHERE oid = trg_reloid
                 ;
 
       FOR src_column IN
              -- Get a list of attributes from the source table
          SELECT atts.attname
            FROM pg_class class
               , pg_attribute atts
           WHERE class.oid = src_reloid
             AND class.oid = atts.attrelid
             AND atts.attnum > 0
 
       LOOP
 
        -- Get the column number with this name from the target table
        SELECT atts.attnum
                INTO trg_attnum
          FROM pg_class class
             , pg_attribute atts
         WHERE class.oid = trg_reloid
           AND class.oid = atts.attrelid
           AND atts.attname=src_column
                 ;
 
              -- Get the statistics for the source column and insert them into pg_statistic
-- RAISE INFO 'DELETING and INSERTING with reloid = %, attnum = %',trg_reloid,trg_attnum;
                DELETE FROM pg_statistic WHERE starelid = trg_reloid AND staattnum = trg_attnum;
 
                INSERT INTO pg_statistic
                     (
          SELECT trg_reloid -- starelid
               , trg_attnum -- staattnum
               , stats.stanullfrac
               , stats.stawidth
               , stats.stadistinct
               , stats.stakind1
               , stats.stakind2
               , stats.stakind3
               , stats.stakind4
               , stats.staop1
               , stats.staop2
               , stats.staop3
               , stats.staop4
               , stats.stanumbers1
               , stats.stanumbers2
               , stats.stanumbers3
               , stats.stanumbers4
               , stats.stavalues1
               , stats.stavalues2
               , stats.stavalues3
               , stats.stavalues4
            FROM pg_class class
               , pg_attribute atts
               , pg_statistic stats
           WHERE class.oid = src_reloid
             AND class.oid = atts.attrelid
             AND atts.attname = src_column
             AND class.oid = stats.starelid
             AND atts.attnum = stats.staattnum
                     ) ;
 
       END LOOP;
 
       RETURN;
END;
$$ LANGUAGE plpgsql VOLATILE;

