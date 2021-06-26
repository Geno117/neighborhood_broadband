-- Web browsing
--
-- This script produces results for total fetch time when accessing a web page
-- (including all embedded objects).
--
-- Like other scripts, this one begins by creating a temporary table of 1st and 99th
-- percentile measurements for each unit. These are used by the main query to act as
-- boundaries for the trimmed mean calculation. The main query is split into multiple
-- sub-selects, one for each timespan that we want to operate over.
--
-- Resulting values are all in microseconds, apart from 'failrate' which is a decimal
-- (range 0-1 inclusive).

DROP TABLE IF EXISTS unit_webget_pct99;
CREATE TABLE unit_webget_pct99 (
  unit_id INT(11),
  perc1 DOUBLE,
  perc99 DOUBLE,
  INDEX (unit_id)
);
INSERT unit_webget_pct99 SELECT unit_id, MEDIAN(fetch_time,2,1), MEDIAN(fetch_time,2,99) FROM curr_webget GROUP BY unit_id;

SELECT u.unit_id,
       a.period, a.fetch_time_min, a.fetch_time_max, a.fetch_time_mean, a.fetch_time_trimmed_mean, a.fetch_time_99_pct, a.fetch_time_median, a.fetch_time_stddev, a.fetch_time_trimmed_stddev, a.samples,
       b.period, b.fetch_time_min, b.fetch_time_max, b.fetch_time_mean, b.fetch_time_trimmed_mean, b.fetch_time_99_pct, b.fetch_time_median, b.fetch_time_stddev, b.fetch_time_trimmed_stddev, b.samples,
       c.period, c.fetch_time_min, c.fetch_time_max, c.fetch_time_mean, c.fetch_time_trimmed_mean, c.fetch_time_99_pct, c.fetch_time_median, c.fetch_time_stddev, c.fetch_time_trimmed_stddev, c.samples,
       d.period, d.fetch_time_min, d.fetch_time_max, d.fetch_time_mean, d.fetch_time_trimmed_mean, d.fetch_time_99_pct, d.fetch_time_median, d.fetch_time_stddev, d.fetch_time_trimmed_stddev, d.samples
FROM (SELECT DISTINCT unit_id FROM curr_webget) u
LEFT JOIN (
   SELECT t.unit_id, 'Off-Peak Mon-Sun' AS period,
          MIN(fetch_time) AS fetch_time_min, MAX(fetch_time) AS fetch_time_max, AVG(fetch_time) AS fetch_time_mean,
          AVG(IF(fetch_time < perc1 OR fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_mean,
          MEDIAN(fetch_time,2,99) AS fetch_time_99_pct,
          MEDIAN(fetch_time) AS fetch_time_median, STDDEV(fetch_time) AS fetch_time_stddev,
          STDDEV(IF(fetch_time < perc1 OR fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_webget t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_webget_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) NOT IN (19,20,21,22) AND successes>0
   GROUP BY t.unit_id
) a ON u.unit_id = a.unit_id
LEFT JOIN (
   SELECT t.unit_id, '24hr Sat-Sun' AS period,
          MIN(fetch_time) AS fetch_time_min, MAX(fetch_time) AS fetch_time_max, AVG(fetch_time) AS fetch_time_mean,
          AVG(IF(fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_mean, MEDIAN(fetch_time,2,99) AS fetch_time_99_pct,
          MEDIAN(fetch_time) AS fetch_time_median, STDDEV(fetch_time) AS fetch_time_stddev,
          STDDEV(IF(fetch_time < perc1 OR fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_webget t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_webget_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (1,7) AND successes>0
   GROUP BY t.unit_id
) b ON u.unit_id = b.unit_id
LEFT JOIN (
   SELECT t.unit_id, '1900-2200 Mon-Fri' AS period,
          MIN(fetch_time) AS fetch_time_min, MAX(fetch_time) AS fetch_time_max, AVG(fetch_time) AS fetch_time_mean,
          AVG(IF(fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_mean, MEDIAN(fetch_time,99) AS fetch_time_99_pct,
          MEDIAN(fetch_time) AS fetch_time_median, STDDEV(fetch_time) AS fetch_time_stddev,
          STDDEV(IF(fetch_time < perc1 OR fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_webget t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_webget_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (19,20,21,22) AND successes>0
   GROUP BY t.unit_id
) c ON u.unit_id = c.unit_id
LEFT JOIN (
   SELECT t.unit_id, '0900-1600 Mon-Fri' AS period,
          MIN(fetch_time) AS fetch_time_min, MAX(fetch_time) AS fetch_time_max, AVG(fetch_time) AS fetch_time_mean,
          AVG(IF(fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_mean, MEDIAN(fetch_time,99) AS fetch_time_99_pct,
          MEDIAN(fetch_time) AS fetch_time_median, STDDEV(fetch_time) AS fetch_time_stddev,
          STDDEV(IF(fetch_time < perc1 OR fetch_time > perc99, NULL, fetch_time)) AS fetch_time_trimmed_stddev,
          SUM(successes+failures) AS samples
   FROM curr_webget t
   INNER JOIN unit_tz ON unit_tz.unit_id = t.unit_id
   LEFT JOIN unit_webget_pct99 a1 ON t.unit_id = a1.unit_id
   WHERE dayofweek(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(t.dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (9,10,11,12,13,14,15,16) AND successes>0
   GROUP BY t.unit_id
) d ON u.unit_id = d.unit_id;

DROP TABLE IF EXISTS unit_webget_pct99;

