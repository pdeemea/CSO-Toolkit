create or replace function generate_text(tlen int) returns varchar as $BODY$
import random
random.seed()
symbols = list('abcdefghijklmnopqrstuvwxyz 0123456789')
return ''.join([ random.choice(symbols) for _ in range(tlen)])
$BODY$
language plpythonu volatile;

--set enforce_virtual_segment_number = 16;
create table temp as
    select id::bigint
        from generate_series(1,10000000) id
distributed by (id);

create table test as
    select  id,
            generate_text(1000)::varchar as randtext
        from temp
distributed by (id);

select count(*) from test where randtext ~ '.*(aaa)|(bbb)|(ccc).*' or randtext ~ '.*[abc]{5}.*' or randtext ~ '[01234]{5,9}' or randtext ~ '[xyz]{10}';
