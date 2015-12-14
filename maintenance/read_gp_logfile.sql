create external web table ext_log
(
    event_time timestamp with time zone,
    user_name varchar(100),
    database_name varchar(100),
    process_id varchar(10),
    thread_id varchar(50),
    remote_host varchar(100),
    remote_port varchar(10),
    session_start_time timestamp with time zone,
    transaction_id int,
    gp_session_id text,
    gp_command_count text,
    gp_segment text,
    slice_id text,
    distr_tranx_id text,
    local_tranx_id text,
    sub_tranx_id text,
    event_severity varchar(10),
    sql_state_code varchar(10),
    event_message text,
    event_detail text,
    event_hint text,
    internal_query text,
    internal_query_pos int,
    event_context text,
    debug_query_string text,
    error_cursor_pos int,
    func_name text,
    file_name text,
    file_line int,
    stack_trace text
)
EXECUTE E'cat $MASTER_DATA_DIRECTORY/pg_log/gpdb-*.csv 2> /dev/null || true' ON MASTER 
FORMAT 'csv' (delimiter ',' null '' escape '"' quote '"')
ENCODING 'UTF8';