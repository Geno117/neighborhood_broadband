-- These queries primarily serve to remove non-MLab results (i.e. those from on-net servers) from the dataset.
-- Additionally, these queries also remove the results of failed tests in many cases, as well as some obvious
-- outliers and anomalous results.

DELETE FROM curr_httpgetmt WHERE successes = 0 OR sequence > 5 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_httppostmt WHERE successes = 0 OR sequence > 5 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_webget WHERE fetch_time <= 0 OR successes = 0 OR fetch_time < 50000 OR fetch_time > 30000000;
DELETE FROM curr_udplatency WHERE (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%') OR successes = 0 OR successes < failures OR successes < 50 OR rtt_min < 1000 OR rtt_max - rtt_min > 300000;
DELETE FROM curr_videostream WHERE successes = 0 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_udpjitter WHERE successes = 0 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_dlping WHERE (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_ulping WHERE (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
-- This removes latency/loss results where packet loss was greater than 10% for any given hour.
-- Loss levels are typically 0.1-0.2%, and at 10% a connection would be almost unusable.

delete from curr_udplatency where successes + failures < 50 OR failures/(successes+failures) > 0.1;
delete from curr_netusage where wan_rx_bytes<0 or wan_tx_bytes<0 or sk_rx_bytes<0 or sk_tx_bytes<0
OR sk_rx_bytes > 100000000000 or sk_tx_bytes > 100000000000 OR wan_rx_bytes>100000000000
OR wan_tx_bytes>100000000000 OR wan_tx_bytes<sk_tx_bytes OR wan_rx_bytes<sk_rx_bytes;
delete from curr_videostream where buffer_filltime > 10000000;
DELETE FROM curr_httpgetmt6 WHERE successes = 0 OR sequence > 5 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_httppostmt6 WHERE successes = 0 OR sequence > 5 OR (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%');
DELETE FROM curr_udplatency6 WHERE (target NOT LIKE '%level3%' and target NOT LIKE '%mlab%') OR successes = 0 OR successes < failures OR successes < 50 OR rtt_min < 1000 OR rtt_max - rtt_min > 300000;
delete from curr_udplatency6 where successes + failures < 50 OR failures/(successes+failures) > 0.1;
-- These queries remove obviously anomalous results (less than zero values).
-- These catches zero or very few results, and is run as a sanity check
-- alone.

delete from curr_httpgetmt where bytes_sec_interval <= 0 OR bytes_sec <= 0;
delete from curr_httppostmt where bytes_sec_interval <= 0 OR bytes_sec <= 0;
delete from curr_udplatency where rtt_avg <= 0;
delete from curr_dlping where rtt_avg <= 0;
delete from curr_ulping where rtt_avg <= 0;
delete from curr_webget where fetch_time <= 0;
delete from curr_udpjitter where jitter_up <= 0 OR jitter_up <= 0;
delete from curr_lct_dl where successes=0 or packets_sent < 1000;
delete from curr_lct_ul where successes=0 or packets_sent < 1000;

