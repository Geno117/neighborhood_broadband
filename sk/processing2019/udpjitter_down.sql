-- Downstream UDP jitter
--
-- This script produces results for downstream UDP jitter measurements (64k/s stream).
--
-- Like other scripts, this one begins by creating a temporary table of 1st and 99th
-- percentile measurements for each unit. These are used by the main query to act as
-- boundaries for the trimmed mean calculation. The main query is split into multiple
-- sub-selects, one for each timespan that we want to operate over.
--
-- Resulting values are all in microseconds, apart from 'failrate' which is a decimal
-- (range 0-1 inclusive).

DROP TABLE IF EXISTS unit_udpjitter_pct99;
CREATE TABLE unit_udpjitter_pct99 (
  unit_id INT(11),
  perc1 DOUBLE,
  perc99 DOUBLE,
  INDEX (unit_id)
);
INSERT unit_udpjitter_pct99 SELECT unit_id, MEDIAN(jitter_down,2,1), MEDIAN(jitter_down,2,99) FROM curr_udpjitter GROUP BY unit_id;

SELECT u.unit_id,
       a.period, a.min, a.max, a.mean, a.trimmed_mean, a.10_pct, a.90_pct, a.8_pct, a.92_pct, a.5_pct, a.95_pct, a.3_pct, a.97_pct, a.99_pct, a.median, a.stddev, a.trimmed_stddev, a.failrate, a.samples,
       b.period, b.min, b.max, b.mean, b.trimmed_mean, b.10_pct, b.90_pct, b.8_pct, b.92_pct, b.5_pct, b.95_pct, b.3_pct, b.97_pct, b.99_pct, b.median, b.stddev, b.trimmed_stddev, b.failrate, b.samples,
       c.period, c.min, c.max, c.mean, c.trimmed_mean, c.10_pct, c.90_pct, c.8_pct, c.92_pct, c.5_pct, c.95_pct, c.3_pct, c.97_pct, c.99_pct, c.median, c.stddev, c.trimmed_stddev, c.failrate, c.samples,
       d.period, d.min, d.max, d.mean, d.trimmed_mean, d.10_pct, d.90_pct, d.8_pct, d.92_pct, d.5_pct, d.95_pct, d.3_pct, d.97_pct, d.99_pct, d.median, d.stddev, d.trimmed_stddev, d.failrate, d.samples
FROM (SELECT DISTINCT unit_id FROM curr_udpjitter) u
LEFT JOIN
(
   SELECT curr_udpjitter.unit_id, 'Off-Peak Mon-Sun' AS period, MIN(jitter_down) AS min, MAX(jitter_down) AS max, AVG(jitter_down) AS mean,
          AVG(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_mean,
          MEDIAN(jitter_down,2,10) AS 10_pct, MEDIAN(jitter_down,2,90) AS 90_pct, MEDIAN(jitter_down,2,8) AS 8_pct, MEDIAN(jitter_down,2,92) AS 92_pct, MEDIAN(jitter_down,2,5) AS 5_pct,
          MEDIAN(jitter_down,2,95) AS 95_pct, MEDIAN(jitter_down,2,3) AS 3_pct, MEDIAN(jitter_down,2,97) AS 97_pct, 
          MEDIAN(jitter_down,2,99) AS 99_pct, MEDIAN(jitter_down) AS median, STDDEV(jitter_down) AS stddev,
                  STDDEV(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_stddev,
          SUM(failures)/SUM(successes+failures) AS failrate, SUM(successes+failures) AS samples
   FROM curr_udpjitter
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_udpjitter.unit_id
   LEFT JOIN unit_udpjitter_pct99 a1 ON curr_udpjitter.unit_id = a1.unit_id
   WHERE successes > 0 AND HOUR(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) NOT IN (19,20,21,22)
   GROUP BY unit_id
) a ON u.unit_id = a.unit_id
LEFT JOIN
(
   SELECT curr_udpjitter.unit_id, '24hr Sat-Sun' AS period, MIN(jitter_down) AS min, MAX(jitter_down) AS max, AVG(jitter_down) AS mean,
          AVG(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_mean, 
          MEDIAN(jitter_down,2,10) AS 10_pct, MEDIAN(jitter_down,2,90) AS 90_pct, MEDIAN(jitter_down,2,8) AS 8_pct, MEDIAN(jitter_down,2,92) AS 92_pct, MEDIAN(jitter_down,2,5) AS 5_pct,
          MEDIAN(jitter_down,2,95) AS 95_pct, MEDIAN(jitter_down,2,3) AS 3_pct, MEDIAN(jitter_down,2,97) AS 97_pct,
          MEDIAN(jitter_down,2,99) AS 99_pct, MEDIAN(jitter_down) AS median, STDDEV(jitter_down) AS stddev,
                  STDDEV(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_stddev,
          SUM(failures)/SUM(successes+failures) AS failrate, SUM(successes+failures) AS samples
   FROM curr_udpjitter
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_udpjitter.unit_id
   LEFT JOIN unit_udpjitter_pct99 a1 ON curr_udpjitter.unit_id = a1.unit_id
   WHERE successes > 0 AND dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (1,7)
   GROUP BY unit_id
) b ON u.unit_id = b.unit_id
LEFT JOIN
(
   SELECT curr_udpjitter.unit_id, '1900-2200 Mon-Fri' AS period, MIN(jitter_down) AS min, MAX(jitter_down) AS max, AVG(jitter_down) AS mean,
          AVG(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_mean, 
          MEDIAN(jitter_down,2,10) AS 10_pct, MEDIAN(jitter_down,2,90) AS 90_pct, MEDIAN(jitter_down,2,8) AS 8_pct, MEDIAN(jitter_down,2,92) AS 92_pct, MEDIAN(jitter_down,2,5) AS 5_pct,
          MEDIAN(jitter_down,2,95) AS 95_pct, MEDIAN(jitter_down,2,3) AS 3_pct, MEDIAN(jitter_down,2,97) AS 97_pct,
          MEDIAN(jitter_down,2,99) AS 99_pct, MEDIAN(jitter_down) AS median, STDDEV(jitter_down) AS stddev,
                  STDDEV(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_stddev,
          SUM(failures)/SUM(successes+failures) AS failrate, SUM(successes+failures) AS samples
   FROM curr_udpjitter
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_udpjitter.unit_id
   LEFT JOIN unit_udpjitter_pct99 a1 ON curr_udpjitter.unit_id = a1.unit_id
   WHERE successes > 0 AND dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (19,20,21,22)
   GROUP BY unit_id
) c ON u.unit_id = c.unit_id
LEFT JOIN
(
   SELECT curr_udpjitter.unit_id, '0900-1600 Mon-Fri' AS period, MIN(jitter_down) AS min, MAX(jitter_down) AS max, AVG(jitter_down) AS mean,
          AVG(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_mean, 
          MEDIAN(jitter_down,2,10) AS 10_pct, MEDIAN(jitter_down,2,90) AS 90_pct, MEDIAN(jitter_down,2,8) AS 8_pct, MEDIAN(jitter_down,2,92) AS 92_pct, MEDIAN(jitter_down,2,5) AS 5_pct,
          MEDIAN(jitter_down,2,95) AS 95_pct, MEDIAN(jitter_down,2,3) AS 3_pct, MEDIAN(jitter_down,2,97) AS 97_pct,
          MEDIAN(jitter_down,2,99) AS 99_pct, MEDIAN(jitter_down) AS median, STDDEV(jitter_down) AS stddev,
                  STDDEV(IF(jitter_down < perc1 OR jitter_down > perc99, NULL, jitter_down)) AS trimmed_stddev,
          SUM(failures)/SUM(successes+failures) AS failrate, SUM(successes+failures) AS samples
   FROM curr_udpjitter
   INNER JOIN unit_tz ON unit_tz.unit_id = curr_udpjitter.unit_id
   LEFT JOIN unit_udpjitter_pct99 a1 ON curr_udpjitter.unit_id = a1.unit_id
   WHERE successes > 0 AND dayofweek(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (2,3,4,5,6) AND HOUR(dtime + INTERVAL (IF(dtime>'2018-03-11',dst,tz)) HOUR) IN (9,10,11,12,13,14,15,16)
   GROUP BY unit_id
) d ON u.unit_id = d.unit_id;

DROP TABLE IF EXISTS unit_udpjitter_pct99;
