-- Create sink table first 

CREATE TABLE IF NOT EXISTS processed_events_aggregated (
            event_time TIMESTAMP(3),
            host VARCHAR,
            ip VARCHAR,
            num_hits BIGINT
);

-- query table for exploration - shows the the number of events per ip per host in a session
SELECT * FROM processed_events_aggregated WHERE host LIKE '%techcreator%';

-- What is the average number of web events of a session from a user on Tech Creator?
-- Since data is mostly ready with number of hits we just need to average over those and filter by techcreator
SELECT 
	AVG(num_hits) AS avg_hits
	FROM processed_events_aggregated
	WHERE host LIKE '%techcreator%';


-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT 
	AVG(num_hits) as avg_hits, 
	host 
	FROM processed_events_aggregated
	WHERE host LIKE '%techcreator%'
	GROUP BY host;
