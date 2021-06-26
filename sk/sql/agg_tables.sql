

-- weekly httpget
DROP TABLE IF EXISTS httpgetspeedweekly;
DROP TABLE IF EXISTS httpgetspeedmonthly;

-- create table httpgetspeedweekly as select unit_id, avg(bytes_sec) as speed, date_trunc('week',dtime) as date from httpget group by unit_id, date;
create table httpgetspeedweekly as select unit_id, avg(bytes_sec) as speed, date_trunc('week',dtime) as date from httpgetmt group by unit_id, date;
--monthly httpget


-- create table httpgetspeedmonthly as select unit_id, avg(bytes_sec) as speed, date_trunc('month',dtime) as date from httpget group by unit_id, date;
create table httpgetspeedmonthly as select unit_id, avg(bytes_sec) as speed, date_trunc('month',dtime) as date from httpgetmt group by unit_id, date;


--weekly    datausage
--monthly   datausage
\copy 