CREATE DATABASE IF NOT EXISTS SONGDB;
USE DATABASE SONGDB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;  

CREATE OR REPLACE STAGE stage_top100
URL = 's3://pvdsongbucket/top100tracks'
STORAGE_INTEGRATION = my_s3_integration
  DIRECTORY = (
    ENABLE = true
    AUTO_REFRESH = true
  );
  

CREATE OR REPLACE TABLE "top100_staging" (
    raw_data VARIANT,
    load_date DATE DEFAULT CURRENT_DATE
);

CREATE OR REPLACE TABLE "top100_latest" (
    chart_date DATE,              
    rank NUMBER,                   
    track_id STRING REFERENCES "track"(track_id),
    metadata VARIANT,              
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (rank)
);

CREATE OR REPLACE TABLE "top100_history" (
    chart_date DATE,               
    rank NUMBER,                   
    track_id STRING REFERENCES "track"(track_id),
    metadata VARIANT,             
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (chart_date, rank)
);


CREATE OR REPLACE PROCEDURE stored_copy_stage_top100()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
COPY INTO "top100_staging" (raw_data)
FROM @stage_top100
FILE_FORMAT = (TYPE = 'JSON');
RETURN 'Copied data from top100 stage successfully';
END;

$$;
CREATE OR REPLACE PROCEDURE stored_proc_is_empty_stage_top100()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET i BOOLEAN DEFAULT IFF(EXISTS (SELECT * FROM "top100_staging"), TRUE, FALSE);
    -- Check if any rows exist in the subquery
    IF (i = TRUE) THEN
        EXECUTE TASK task_load_top100;
        RETURN 'Task loading top100 runned';
    END IF;
    RETURN 'Error 1001: No data, stopped';
END;
$$;

CREATE OR REPLACE PROCEDURE load_top100_latest()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO "artist" AS tgt
USING (
  SELECT
    MD5(LOWER(TRIM(value:"artist_name"::STRING))) AS artist_id,
    INITCAP(TRIM(value:"artist_name"::STRING)) AS artist_name
  FROM "top100_staging",
       LATERAL FLATTEN(input => raw_data)
  WHERE value:"artist_name" IS NOT NULL
) AS src
ON tgt.artist_id = src.artist_id
WHEN NOT MATCHED THEN
  INSERT (artist_id, name)
  VALUES (src.artist_id, src.artist_name);
  
MERGE INTO "track" AS t
USING (
  SELECT
    MD5(LOWER(TRIM(value:"artist_name"::STRING))) AS artist_id,
    MD5(LOWER(TRIM(value:"track_name"::STRING)) || LOWER(TRIM(value:"artist_name"::STRING))) AS track_id,
    value:"track_name"::STRING AS track_name,
    value AS raw_data
  FROM "top100_staging",
       LATERAL FLATTEN(input => raw_data)
  WHERE value:"track_name" IS NOT NULL
) AS s
ON t.track_id = s.track_id
WHEN NOT MATCHED THEN
  INSERT (track_id, track_name, artist_id)
  VALUES (s.track_id, s.track_name, s.artist_id);

INSERT INTO "top100_history" (rank, track_id, chart_date)
SELECT rank, track_id, chart_date
FROM "top100_latest";

DELETE FROM "top100_latest";

INSERT INTO "top100_latest" (rank, track_id, chart_date)
SELECT
value:"rank"::INTEGER AS rank,
MD5(LOWER(TRIM(value:"track_name"::STRING)) || LOWER(TRIM(value:"artist_name"::STRING))) AS track_id,
value:"date"::DATE AS chart_date
FROM "top100_staging",
   LATERAL FLATTEN(input => "top100_staging".raw_data)
WHERE rank IS NOT NULL;
RETURN 'Top 100 latest table updated.';
END;
$$;

CREATE OR REPLACE PROCEDURE stored_proc_clear_top100_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM "top100_staging";
    RETURN 'Completed transfer data';
END;
$$;

CREATE OR REPLACE TASK task_copy_stage_top100
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 MINUTE'
AS CALL stored_copy_stage_top100();

CREATE OR REPLACE TASK task_is_empty_stage_top100
WAREHOUSE = COMPUTE_WH
SCHEDULE = '30 MINUTE'
AS CALL stored_proc_is_empty_stage_top100();

CREATE OR REPLACE TASK task_load_top100
WAREHOUSE = COMPUTE_WH
AS CALL load_top100_latest();

CREATE OR REPLACE TASK task_clear_top100
WAREHOUSE = COMPUTE_WH
AFTER task_load_top100
AS CALL stored_proc_clear_top100_stage();

ALTER TASK task_copy_stage_top100 SUSPEND;
ALTER TASK task_is_empty_stage_top100 SUSPEND;
ALTER TASK task_load_top100 SUSPEND;
ALTER TASK task_clear_top100 SUSPEND;

ALTER TASK task_copy_stage_top100 RESUME;
ALTER TASK task_clear_top100 RESUME;
ALTER TASK task_is_empty_stage_top100 RESUME;
-- call stored_copy_stage_top100();
-- call stored_proc_is_empty_stage_top100();
-- call load_top100_latest();
-- call stored_proc_clear_top100_stage();

-- CREATE OR REPLACE TABLE duplicate_table AS
-- SELECT DISTINCT *
-- FROM "artist"
-- WHERE artist_id IN (
--     SELECT artist_id
--     FROM "artist"
--     GROUP BY artist_id
--     HAVING COUNT(artist_id) > 1
-- );


-- DELETE FROM "artist"
-- WHERE artist_id
-- IN (SELECT artist_id
-- FROM duplicate_table);

-- INSERT INTO "artist"
-- SELECT *
-- FROM duplicate_table;

-- DROP TABLE duplicate_table;

-- CREATE OR REPLACE TABLE duplicate_table AS
-- SELECT DISTINCT *
-- FROM "track"
-- WHERE track_id IN (
--     SELECT track_id
--     FROM "track"
--     GROUP BY track_id
--     HAVING COUNT(track_id) > 1
-- );


-- DELETE FROM "track"
-- WHERE track_id
-- IN (SELECT track_id
-- FROM duplicate_table);

-- INSERT INTO "track"
-- SELECT *
-- FROM duplicate_table;

-- DROP TABLE duplicate_table;

-- INSERT INTO "top100_history" (rank, track_id, chart_date)
-- SELECT rank, track_id, chart_date
-- FROM "top100_latest";

-- DELETE FROM "top100_latest";