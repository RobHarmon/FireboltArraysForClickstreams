--first, we create an external table pointing at the s3 location of all_events
--of our clickstream logs.  They're Parquet, so they'll go fast.

DROP TABLE IF EXISTS ext_click_stream;

	CREATE EXTERNAL TABLE ext_click_stream (
		event_time DATETIME
		,ad_id INTEGER
		,session_id string
		,viewer_id string
		,event_type INTEGER
		,channel_id INTEGER)
		URL = 's3://my_bucket/'
        OBJECT_PATTERN= 'clickstream*.parquet'
        TYPE = (PARQUET);
		;
		
		
--next, we create a stagig table because we're going to be deriving the session_start
--we could do this directly against the external table, but it would be slow
--you'll also notice I partitioned this table by a "partitionid".  More on that below.

DROP TABLE IF EXISTS stage_click_stream;

	CREATE fact TABLE stage_click_stream (
		partitionid INT NOT NULL
		,event_time TIMESTAMP NOT NULL
		,ad_id INT NOT NULL
		,session_id TEXT NOT NULL
		,viewer_id TEXT NOT NULL
		,event_type INT NOT NULL
		,channel_id INT NOT NULL
		,source_file_name TEXT NOT NULL
		,source_file_timestamp TIMESTAMP NOT NULL
		) PRIMARY INDEX partitionid
		,session_id
		,event_time PARTITION BY partitionid;

--since we're going to be looking for the min(event_time) from the staging table for any
--given session, it's going to help by materializing that aggregate

DROP aggregating INDEX if exists ix_stage_click_stream_agg_sessionid_event_time;
CREATE
	AND generate aggregating INDEX ix_stage_click_stream_agg_sessionid_event_time ON stage_click_stream (
	session_id
	,min(event_time)
	);

--and the target table.  You'll notice there isn't much done to it.  The table would look pretty similar on
--any platform.  The magic, here, isn't in the table itself so we're free to normalize as necessary.

DROP TABLE IF EXISTS click_stream;
	CREATE fact TABLE click_stream (
		session_start TIMESTAMP NOT NULL
		,event_time TIMESTAMP NOT NULL
		,ad_id INT NULL  --adrequests don't have an adid yet, so they can be null
		,campaign_id int null  --I'm enriching things here, this wasn't in the log so I'll look it up at load
		,advertiser_id int null  --same as above
		,session_id TEXT NOT NULL
		,viewer_id TEXT NOT NULL
		,channel_id int NOT NULL  --this is the distributors distribution channel
		,content_distributor_id int not null  --This is the company or group that brought me the viewer, I'm buying eyeballs
		,event_type INT NOT NULL  --here, we've surrogated events, 1 for request, 2 for impression, 3 for click etc.
		,source_file_name TEXT NOT NULL
		,source_file_timestamp TIMESTAMP NOT NULL
		) PRIMARY INDEX session_start  --we're generally going to be querying by time, so session start is an awesome primary index
		,ad_id;

--this might seem an odd index, but it's possible to get parts of a user session in two different batches
--so the session start may be earlier than the data I've staged

CREATE
	AND generate aggregating INDEX ix_click_stream_agg_sessionid_event_time ON click_stream (
	session_id
	,min(event_time)
	);


--this is where the real cool stuff happens.  The NEST function is an aggregate function that creates arrays of all 
--of the event_types for a given aggregate key combination.  This will be materialized in the background and maintained
--automatically as I ingest new click_stream logs.  When I write a query and the compiler notices it has an aggregating
--index that can fulfill the query, it'll automatically take advantage of it.


CREATE
	AND generate aggregating INDEX ix_click_stream_arrays ON click_stream (
	session_start
	,session_id
	,ad_id
	,nest(event_type)
	,nest(DISTINCT event_type)
	,count(*)
	);


--lets start loading data in, first I bring in all the clickstream rows from the external table for all
--the files I haven't loaded yet.

--there's no logical reason to partition this table, I just don't want to go through the trouble of recreating
--the table to get rid of data at the end of the batch/microbatch.  Dropping the partition serves as a truncate.

INSERT INTO stage_click_stream
SELECT 1 AS partitionmid  
	,event_time
	,ad_id
	,session_id
	,viewer_id
	,event_type
    ,channel_id
	,source_file_name
	,source_file_timestamp
FROM ext_click_stream
WHERE source_file_name NOT IN (
		SELECT source_file_name
		FROM click_stream
		);
		
		
--lets populate the internal table.  If the session was partially loaded in the previous batch, we'll use that
--session start time, if it's a new session, we'll pull it from staging
--you can also see I'm pulling in some data enrichment, the advertiser and distributor can be derived from
--the ad_id and channel_id respectively.

--I'm using the SOURCE_FILE_NAME metadata column from the external table to load only those files we haven't seen
--before

--this script can be run repeatedly and it'll only load new data, continuous ingestion the easy way		

INSERT INTO click_stream
WITH existing_session_starts AS (
		SELECT session_id
			,min(event_time) AS session_start
		FROM click_stream
		WHERE session_id IN (
				SELECT session_id
				FROM stage_click_stream
				)
		GROUP BY session_id
		)
	,new_session_starts AS (
		SELECT session_id
			,min(event_time) AS stage_session_start
		FROM stage_click_stream
		GROUP BY session_id
		)

SELECT coalesce(es.session_start, ns.stage_session_start) AS session_start
	,event_time
	,ad_id
	,c.campaign_id
	,c.advertiser_id
	,sc.session_id
	,viewer_id
	,channel_id
	,ch.content_distributor_id
	,event_type
	,source_file_name
	,source_file_timestamp
FROM stage_click_stream sc
LEFT OUTER JOIN existing_session_starts es ON es.session_id = sc.session_id
INNER JOIN new_session_starts ns ON ns.session_id = sc.session_id
left outer join ad a on a.ad_id = click_stream.ad_id
left outer join campaign c on campaign.campaign_id = a.campaign_id
left outer join channel ch on ch.channel_id = clickstream.channel_id
WHERE source_file_name NOT IN (
		SELECT source_file_name
		FROM click_stream
		)

--Then we clear out the staging table:
alter table stage_click_stream drop partition 1;

--now that data's loading, and I've got all my indexes in place we can run our query to find
--click fraud.

--we could create similar aggregating indexes at the advertizer level, channel level and distributor levels
--as well so we can query higher level summaries much faster

--all events will give me an array of all the eventtypes in the session, sometimes we see 3 clicks for a single impression etc.
--distinct events is pretty self explanatory, it's a distinct array of the event types that have completed for the session
--ARRAY_COUNT is a lamda function that returns the number of instances of a literal in an array.

WITH click_summary
AS (
	SELECT session_start
		,session_id
		,ad_id
		,nest(event_type) AS all_events
		,nest(DISTINCT event_type) AS distinct_events
		,count(*)
	FROM click_stream
	GROUP BY session_start
		,session_id
		,ad_id
	)
SELECT ARRAY_COUNT(x -> x = 2, all_events) AS fraudulant_click_count
	,*
FROM click_summary
WHERE CONTAINS (
		distinct_events
		,2
		)
	AND NOT CONTAINS (
		distinct_events
		,9
		)
	AND session_start >= dateadd('day', - 1, current_timestamp);
