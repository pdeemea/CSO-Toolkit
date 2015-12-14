#!/bin/bash

source ~/.bash_profile
source ~/.bashrc

if [ -z "$1" ]; then
    echo "No database name supplied"
    exit 1
fi

DBNAME="$1"

VCOMMAND="VACUUM FULL"
psql -tc "select '$VCOMMAND' || ' pg_catalog.' || relname || ';'
from pg_class a,pg_namespace b where a.relnamespace=b.oid and
b.nspname='pg_catalog' and a.relkind='r'" $DBNAME | psql -a $DBNAME

VCOMMAND="REINDEX TABLE "
psql -tc "select distinct '$VCOMMAND' || ' pg_catalog.' || relname || ';'
from pg_class as c, pg_namespace as n, pg_index as i
where c.relnamespace = n.oid and i.indrelid = c.oid and n.nspname = 'pg_catalog'" $DBNAME | psql -a $DBNAME
