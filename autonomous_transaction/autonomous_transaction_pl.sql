CREATE FUNCTION autonomous_transaction(text) RETURNS text AS $BODY$
#=============================================================================================
#created by:    andreas scherbaum
#creation date: May 19, 2011
#description:   execute autonomous transaction (not affected by roolback of parent function)
#parameters:    text: transaction to execute
#returns:       text
#=============================================================================================

    use strict;
    use FileHandle;
    use DBI;

    # fetch the query from the argument list
    my $query = shift;

    # open database connection, use environment variables provided by Greenplum
    # note: no password is used, add suitable line in pg_hba.conf
    # note: the values in %ENV are the values from /usr/local/greenplum-db/greenplum_path.sh
    # note: version with "host" does not work as the greenplum violates this connection
    #       as somehow it treats this as internal "if (PG_PROTOCOL_MAJOR(port->proto) == 3 && port->proto >> 28 == 7)"
    #my $db = DBI->connect('DBI:Pg:dbname=' . $ENV{'PGDATABASE'} . ';host=' . $ENV{'PGHOST'} . ';port=' . $ENV{'PGPORT'},
    my $db = DBI->connect('DBI:Pg:dbname=' . $ENV{'PGDATABASE'} . ';port=' . $ENV{'PGPORT'},
                          $ENV{'PGUSER'}, '',
                          {PrintWarn => 0, PrintError => 0, RaiseError => 0});
    # if the connection failed return error message
    if (!$db) {
        #elog(ERROR, "failed to open database connection: " . $DBI::errstr);
        return $DBI::errstr;
    }

    # execute query
    if (!$db->do($query)) {
        #elog(ERROR, "failed to insert log message: " . $DBI::errstr);
        return $DBI::errstr;
    }

    # cleanup and exit
    $db->disconnect;
    return '';
$BODY$
LANGUAGE plperlu
volatile;
