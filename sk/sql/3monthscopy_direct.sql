
\COPY datausage    FROM '~/data/201807/curr_datausage.csv'       WITH (FORMAT CSV, HEADER);
\COPY datausage    FROM '~/data/201808/curr_datausage.csv'       WITH (FORMAT CSV, HEADER);
\COPY datausage    FROM '~/data/201809/curr_datausage.csv'       WITH (FORMAT CSV, HEADER);

\COPY httpget    FROM '~/data/201807/curr_httpget.csv'       WITH (FORMAT CSV, HEADER);
\COPY httpget    FROM '~/data/201808/curr_httpget.csv'       WITH (FORMAT CSV, HEADER);
\COPY httpget    FROM '~/data/201809/curr_httpget.csv'       WITH (FORMAT CSV, HEADER);

\COPY httpgetmt    FROM '~/data/201807/curr_httpgetmt.csv'       WITH (FORMAT CSV, HEADER);
\COPY httpgetmt    FROM '~/data/201808/curr_httpgetmt.csv'       WITH (FORMAT CSV, HEADER);
\COPY httpgetmt    FROM '~/data/201809/curr_httpgetmt.csv'       WITH (FORMAT CSV, HEADER);

-- \COPY dlping       FROM 'data/curr_dlping.csv'       WITH (FORMAT CSV, HEADER); 
-- \COPY dns          FROM 'data/curr_dns.csv'          WITH (FORMAT CSV, HEADER); 
-- \COPY httpget      FROM 'data/curr_httpget.csv'      WITH (FORMAT CSV, HEADER); 
-- \COPY httpgetmt    FROM 'data/curr_httpgetmt.csv'    WITH (FORMAT CSV, HEADER); 
-- \COPY httpgetmt6   FROM 'data/curr_httpgetmt6.csv'   WITH (FORMAT CSV, HEADER); 
-- \COPY httppost     FROM 'data/curr_httppost.csv'     WITH (FORMAT CSV, HEADER); 
-- \COPY httppostmt   FROM 'data/curr_httppostmt.csv'   WITH (FORMAT CSV, HEADER); 
-- \COPY httppostmt6  FROM 'data/curr_httppostmt6.csv'  WITH (FORMAT CSV, HEADER); 
-- \COPY netusage     FROM 'data/curr_netusage.csv'     WITH (FORMAT CSV, HEADER); 
-- \COPY ping         FROM 'data/curr_ping.csv'         WITH (FORMAT CSV, HEADER); 
-- \COPY udpcloss     FROM 'data/curr_udpcloss.csv'     WITH (FORMAT CSV, HEADER); 
-- \COPY udpjitter    FROM 'data/curr_udpjitter.csv'    WITH (FORMAT CSV, HEADER); 
-- \COPY udplatency   FROM 'data/curr_udplatency.csv'   WITH (FORMAT CSV, HEADER); 
-- \COPY udplatency6  FROM 'data/curr_udplatency6.csv'  WITH (FORMAT CSV, HEADER); 
-- \COPY ulping       FROM 'data/curr_ulping.csv'       WITH (FORMAT CSV, HEADER); 
-- \COPY videostream  FROM 'data/curr_videostream.csv'  WITH (FORMAT CSV, HEADER); 
-- \COPY webget       FROM 'data/curr_webget.csv'       WITH (FORMAT CSV, HEADER); 

