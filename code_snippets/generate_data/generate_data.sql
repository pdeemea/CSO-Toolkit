create or replace function generate_integer(length int) returns varchar as $BODY$
import random
res = ''
for a in range(length):
	res += str(int(random.random()*10))
return res
$BODY$
language plpythonu
volatile;

create or replace function generate_string(length int, use_upper int) returns varchar as $BODY$
import random
alpha = 'abcdefghijklmnopqrstuvwxyz'
alpha_len = len(alpha)
res = ''
for a in range(length):
	if use_upper > 0 and random.random() < 0.5:
		res += alpha[int(random.random()*alpha_len)].upper()
	else:
		res += alpha[int(random.random()*alpha_len)]
return res
$BODY$
language plpythonu
volatile;

create or replace function generate_string(length int) returns varchar as $BODY$
	select generate_string($1, 1)
$BODY$
language sql
volatile;

create or replace function generate_domain() returns varchar as $BODY$
import random
domains = ['com', 'ru', 'info', 'co.il']
domains_len = len(domains)
return domains[ int(random.random()*domains_len) ]
$BODY$
language plpythonu
volatile;

create or replace function generate_email(username int, domainname int) returns varchar as $BODY$
select generate_string($1) || '@' || generate_string($2,0) || '.' || generate_domain()
$BODY$
language sql
volatile;

create or replace function generate_point (lat_min float8, lat_max float8, lon_min float8, lon_max float8) returns geometry as $BODY$
select ST_GeomFromText('POINT(' || ($1 + round((random()*($2-$1))::numeric,5))::varchar || ' ' || ($3 + round((random()*($4-$3))::numeric,5))::varchar || ')')
$BODY$
language sql
volatile;