SELECT u.unit_id,
       a.period, a.min, a.max, a.mean, a.total, a.samples,
       b.period, b.min, b.max, b.mean, b.total, b.samples,
       c.period, c.min, c.max, c.mean, c.total, c.samples,
       d.period, d.min, d.max, d.mean, d.total, d.samples
FROM (SELECT DISTINCT unit_id FROM curr_netusage) u
LEFT JOIN
(
   SELECT curr_netusage.unit_id, '24hr Mon-Sun' AS period, MIN(wan_rx_bytes-sk_rx_bytes) AS min, MAX(wan_rx_bytes-sk_rx_bytes) AS max, AVG(wan_rx_bytes-sk_rx_bytes) AS mean,
          SUM(wan_rx_bytes-sk_rx_bytes) as total, COUNT(*) AS samples
   FROM curr_netusage
   GROUP BY unit_id
) a ON u.unit_id = a.unit_id
LEFT JOIN
(
   SELECT curr_netusage.unit_id, '24hr Sat-Sun' AS period, MIN(wan_rx_bytes-sk_rx_bytes) AS min, MAX(wan_rx_bytes-sk_rx_bytes) AS max, AVG(wan_rx_bytes-sk_rx_bytes) AS mean,
          SUM(wan_rx_bytes-sk_rx_bytes) as total, COUNT(*) AS samples
   FROM curr_netusage
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_netusage.unit_id
   WHERE dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (1,7)
   GROUP BY unit_id
) b ON u.unit_id = b.unit_id
LEFT JOIN
(
   SELECT curr_netusage.unit_id, '1900-2200 Mon-Fri' AS period, MIN(wan_rx_bytes-sk_rx_bytes) AS min, MAX(wan_rx_bytes-sk_rx_bytes) AS max, AVG(wan_rx_bytes-sk_rx_bytes) AS mean,
          SUM(wan_rx_bytes-sk_rx_bytes) as total, COUNT(*) AS samples
   FROM curr_netusage
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_netusage.unit_id
   WHERE dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (19,20,21,22)
   GROUP BY unit_id
) c ON u.unit_id = c.unit_id
LEFT JOIN
(
   SELECT curr_netusage.unit_id, '0900-1600 Mon-Fri' AS period, MIN(wan_rx_bytes-sk_rx_bytes) AS min, MAX(wan_rx_bytes-sk_rx_bytes) AS max, AVG(wan_rx_bytes-sk_rx_bytes) AS mean,
          SUM(wan_rx_bytes-sk_rx_bytes) as total, COUNT(*) AS samples
   FROM curr_netusage
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_netusage.unit_id
   WHERE dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (9,10,11,12,13,14,15,16)
   GROUP BY unit_id
) d ON u.unit_id = d.unit_id;
