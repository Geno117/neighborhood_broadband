DROP TABLE IF EXISTS unit_httppostmt_pct99;
CREATE TABLE unit_httppostmt_pct99 (
  unit_id INT(11),
  perc1 DOUBLE,
  perc99 DOUBLE,
  INDEX (unit_id)
);
INSERT unit_httppostmt_pct99
SELECT unit_id, MEDIAN(bytes_sec,2,1), MEDIAN(bytes_sec,2,99) FROM curr_httppostmt GROUP BY unit_id;



SELECT u.unit_id,
       a.period,
       a.sustained_max, a.sustained_mean, a.sustained_trimmed_mean,
       a.sustained_10_pct, a.sustained_90_pct, a.sustained_20_pct, a.sustained_30_pct, a.sustained_5_pct, a.sustained_95_pct, a.sustained_3_pct, a.sustained_97_pct, 
       a.sustained_99_pct, a.sustained_median, a.sustained_stddev, a.sustained_trimmed_stddev, a.samples,

       b.period,
       b.sustained_max, b.sustained_mean, b.sustained_trimmed_mean,
       b.sustained_10_pct, b.sustained_90_pct, b.sustained_20_pct, b.sustained_30_pct, b.sustained_5_pct, b.sustained_95_pct, b.sustained_3_pct, b.sustained_97_pct, 
       b.sustained_99_pct, b.sustained_median, b.sustained_stddev, b.sustained_trimmed_stddev, b.samples,

       c.period,
       c.sustained_max, c.sustained_mean, c.sustained_trimmed_mean,
       c.sustained_10_pct, c.sustained_90_pct, c.sustained_20_pct, c.sustained_30_pct, c.sustained_5_pct, c.sustained_95_pct, c.sustained_3_pct, c.sustained_97_pct, 
       c.sustained_99_pct, c.sustained_median, c.sustained_stddev, c.sustained_trimmed_stddev, c.samples,

       d.period,
       d.sustained_max, d.sustained_mean, d.sustained_trimmed_mean,
       d.sustained_10_pct, d.sustained_90_pct, d.sustained_20_pct, d.sustained_30_pct, d.sustained_5_pct, d.sustained_95_pct, d.sustained_3_pct, d.sustained_97_pct, 
       d.sustained_99_pct, d.sustained_median, d.sustained_stddev, d.sustained_trimmed_stddev, d.samples,

       e.period, e.sustained_trimmed_mean, e.sustained_trimmed_stddev, e.samples,
       f.period, f.sustained_trimmed_mean, f.sustained_trimmed_stddev, f.samples,
       g.period, g.sustained_trimmed_mean, g.sustained_trimmed_stddev, g.samples,
       h.period, h.sustained_trimmed_mean, h.sustained_trimmed_stddev, h.samples,
       i.period, i.sustained_trimmed_mean, i.sustained_trimmed_stddev, i.samples,
       j.period, j.sustained_trimmed_mean, j.sustained_trimmed_stddev, j.samples,
       k.period, k.sustained_trimmed_mean, k.sustained_trimmed_stddev, k.samples,
       l.period, l.sustained_trimmed_mean, l.sustained_trimmed_stddev, l.samples,
       m.period, m.sustained_trimmed_mean, m.sustained_trimmed_stddev, m.samples,
       n.period, n.sustained_trimmed_mean, n.sustained_trimmed_stddev, n.samples,
       o.period, o.sustained_trimmed_mean, o.sustained_trimmed_stddev, o.samples,
       p.period, p.sustained_trimmed_mean, p.sustained_trimmed_stddev, p.samples,
       h1.period, h1.sustained_trimmed_mean, h1.sustained_trimmed_stddev, h1.samples,
       h2.period, h2.sustained_trimmed_mean, h2.sustained_trimmed_stddev, h2.samples,
       h3.period, h3.sustained_trimmed_mean, h3.sustained_trimmed_stddev, h3.samples,
       h4.period, h4.sustained_trimmed_mean, h4.sustained_trimmed_stddev, h4.samples,
       h5.period, h5.sustained_trimmed_mean, h5.sustained_trimmed_stddev, h5.samples,
       h6.period, h6.sustained_trimmed_mean, h6.sustained_trimmed_stddev, h6.samples,
       h7.period, h7.sustained_trimmed_mean, h7.sustained_trimmed_stddev, h7.samples,
       h8.period, h8.sustained_trimmed_mean, h8.sustained_trimmed_stddev, h8.samples,
       h9.period, h9.sustained_trimmed_mean, h9.sustained_trimmed_stddev, h9.samples

FROM (SELECT DISTINCT unit_id FROM curr_httppostmt) u
LEFT JOIN (
   SELECT t.unit_id, 'Off-Peak Mon-Sun' AS period,
          MIN(bytes_sec) AS sustained_min, MAX(bytes_sec) AS sustained_max, AVG(bytes_sec) AS sustained_mean,
          AVG(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, MEDIAN(bytes_sec,2,99) AS sustained_99_pct,
          MEDIAN(bytes_sec,2,10) AS sustained_10_pct, MEDIAN(bytes_sec,2,90) AS sustained_90_pct, MEDIAN(bytes_sec,2,20) AS sustained_20_pct, MEDIAN(bytes_sec,2,30) AS sustained_30_pct,
          MEDIAN(bytes_sec,2,5) AS sustained_5_pct, MEDIAN(bytes_sec,2,95) AS sustained_95_pct, MEDIAN(bytes_sec,2,3) AS sustained_3_pct, MEDIAN(bytes_sec,2,97) AS sustained_97_pct,
          MEDIAN(bytes_sec) AS sustained_median, STDDEV(bytes_sec) AS sustained_stddev, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) NOT IN (19,20,21,22) AND t.sequence=0
   GROUP BY t.unit_id
) a ON u.unit_id = a.unit_id
LEFT JOIN (
   SELECT t.unit_id, '24hr Sat-Sun' AS period,
          MIN(bytes_sec) AS sustained_min, MAX(bytes_sec) AS sustained_max, AVG(bytes_sec) AS sustained_mean,
          AVG(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, MEDIAN(bytes_sec,2,99) AS sustained_99_pct,
          MEDIAN(bytes_sec,2,10) AS sustained_10_pct, MEDIAN(bytes_sec,2,90) AS sustained_90_pct, MEDIAN(bytes_sec,2,20) AS sustained_20_pct, MEDIAN(bytes_sec,2,30) AS sustained_30_pct,
          MEDIAN(bytes_sec,2,5) AS sustained_5_pct, MEDIAN(bytes_sec,2,95) AS sustained_95_pct, MEDIAN(bytes_sec,2,3) AS sustained_3_pct, MEDIAN(bytes_sec,2,97) AS sustained_97_pct,
          MEDIAN(bytes_sec) AS sustained_median, STDDEV(bytes_sec) AS sustained_stddev, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (1,7) AND t.sequence=0
   GROUP BY t.unit_id
) b ON u.unit_id = b.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1900-2200 Mon-Fri' AS period,
          MIN(bytes_sec) AS sustained_min, MAX(bytes_sec) AS sustained_max, AVG(bytes_sec) AS sustained_mean,
          AVG(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, MEDIAN(bytes_sec,2,99) AS sustained_99_pct,
          MEDIAN(bytes_sec,2,10) AS sustained_10_pct, MEDIAN(bytes_sec,2,90) AS sustained_90_pct, MEDIAN(bytes_sec,2,20) AS sustained_20_pct, MEDIAN(bytes_sec,2,30) AS sustained_30_pct,
          MEDIAN(bytes_sec,2,5) AS sustained_5_pct, MEDIAN(bytes_sec,2,95) AS sustained_95_pct, MEDIAN(bytes_sec,2,3) AS sustained_3_pct, MEDIAN(bytes_sec,2,97) AS sustained_97_pct,
          MEDIAN(bytes_sec) AS sustained_median, STDDEV(bytes_sec) AS sustained_stddev, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (19,20,21,22) AND t.sequence=0
   GROUP BY t.unit_id
) c ON u.unit_id = c.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0900-1600 Mon-Fri' AS period,
          MIN(bytes_sec) AS sustained_min, MAX(bytes_sec) AS sustained_max, AVG(bytes_sec) AS sustained_mean,
          AVG(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, MEDIAN(bytes_sec,2,99) AS sustained_99_pct,
          MEDIAN(bytes_sec,2,10) AS sustained_10_pct, MEDIAN(bytes_sec,2,90) AS sustained_90_pct, MEDIAN(bytes_sec,2,20) AS sustained_20_pct, MEDIAN(bytes_sec,2,30) AS sustained_30_pct,
          MEDIAN(bytes_sec,2,5) AS sustained_5_pct, MEDIAN(bytes_sec,2,95) AS sustained_95_pct, MEDIAN(bytes_sec,2,3) AS sustained_3_pct, MEDIAN(bytes_sec,2,97) AS sustained_97_pct,
          MEDIAN(bytes_sec) AS sustained_median, STDDEV(bytes_sec) AS sustained_stddev, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (9,10,11,12,13,14,15,16) AND t.sequence=0
   GROUP BY t.unit_id
) d ON u.unit_id = d.unit_id

LEFT JOIN (
   SELECT t.unit_id, '0000-0200 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (0,1)
   GROUP BY t.unit_id
) e ON u.unit_id = e.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0200-0400 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3)
   GROUP BY t.unit_id
) f ON u.unit_id = f.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0400-0600 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (4,5)
   GROUP BY t.unit_id
) g ON u.unit_id = g.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0600-0800 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (6,7)
   GROUP BY t.unit_id
) h ON u.unit_id = h.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0800-1000 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (8,9)
   GROUP BY t.unit_id
) i ON u.unit_id = i.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1000-1200 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (10,11)
   GROUP BY t.unit_id
) j ON u.unit_id = j.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1200-1400 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (12,13)
   GROUP BY t.unit_id
) k ON u.unit_id = k.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1400-1600 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (14,15)
   GROUP BY t.unit_id
) l ON u.unit_id = l.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1600-1800 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (16,17)
   GROUP BY t.unit_id
) m ON u.unit_id = m.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1800-2000 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (18,19)
   GROUP BY t.unit_id
) n ON u.unit_id = n.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2000-2200 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (20,21)
   GROUP BY t.unit_id
) o ON u.unit_id = o.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2200-0000 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (22,23)
   GROUP BY t.unit_id
) p ON u.unit_id = p.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0000-0600 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (0,1,2,3,4,5)
   GROUP BY t.unit_id
) h1 ON u.unit_id = h1.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0600-1200 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (6,7,8,9,10,11)
   GROUP BY t.unit_id
) h2 ON u.unit_id = h2.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1200-1800 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (12,13,14,15,16,17)
   GROUP BY t.unit_id
) h3 ON u.unit_id = h3.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1800-1900 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (18)
   GROUP BY t.unit_id
) h4 ON u.unit_id = h4.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1900-2000 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (19)
   GROUP BY t.unit_id
) h5 ON u.unit_id = h5.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2000-2100 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (20)
   GROUP BY t.unit_id
) h6 ON u.unit_id = h6.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2100-2200 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (21)
   GROUP BY t.unit_id
) h7 ON u.unit_id = h7.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2200-2300 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (22)
   GROUP BY t.unit_id
) h8 ON u.unit_id = h8.unit_id
LEFT JOIN (
   SELECT t.unit_id, '2300-0000 Mon-Sun' AS period,
          AVG(IF(bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_mean, STDDEV(IF(bytes_sec < perc1 OR bytes_sec > perc99, NULL, bytes_sec)) AS sustained_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_httppostmt t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_httppostmt_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (23)
   GROUP BY t.unit_id
) h9 ON u.unit_id = h9.unit_id;

DROP TABLE IF EXISTS unit_httppostmt_pct99;

