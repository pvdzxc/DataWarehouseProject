CREATE DATABASE IF NOT EXISTS SONGDB;
USE DATABASE SONGDB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;  

CREATE OR REPLACE STAGE artist_stage
URL = 's3://pvdsongbucket/artists'
STORAGE_INTEGRATION = my_s3_integration
  DIRECTORY = (
    ENABLE = true
    AUTO_REFRESH = true
  );
  
CREATE OR REPLACE STAGE track_stage
URL = 's3://pvdsongbucket/tracks'
STORAGE_INTEGRATION = my_s3_integration
  DIRECTORY = (
    ENABLE = true
    AUTO_REFRESH = true
  );

CREATE OR REPLACE TABLE "stage_artist" (
    raw_data VARIANT,
    load_date DATE DEFAULT CURRENT_DATE
);
CREATE OR REPLACE TABLE "stage_track" (
    raw_data VARIANT,
    load_date DATE DEFAULT CURRENT_DATE
);


CREATE OR REPLACE TABLE "artist" (
    artist_id STRING PRIMARY KEY,
    name STRING,
    genres ARRAY,
    popularity NUMBER,
    followers NUMBER,
    external_url STRING,
    metadata VARIANT, -- for keeping raw or extra data if needed
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
CREATE OR REPLACE TABLE "track" (
    track_id STRING PRIMARY KEY,
    track_name STRING,
    duration_ms NUMBER,
    explicit BOOLEAN,
    popularity NUMBER,
    album_name STRING,
    release_date DATE,
    artist_id STRING REFERENCES "artist"(artist_id),
    genres VARIANT,              -- list of genres assigned to this track
    external_url STRING,
    metadata VARIANT,            -- raw or extended track data
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE stored_copy_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
COPY INTO "stage_artist" (raw_data)
FROM @artist_stage
FILE_FORMAT = (TYPE = 'JSON');

COPY INTO "stage_track" (raw_data)
FROM @track_stage
FILE_FORMAT = (TYPE = 'JSON');
RETURN 'Copied data from artist_stage and track_stage successfully';
END;

$$;
CREATE OR REPLACE PROCEDURE stored_proc_is_empty_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET i BOOLEAN DEFAULT IFF(EXISTS (SELECT * FROM "stage_artist"), TRUE, FALSE);
    -- Check if any rows exist in the subquery
    IF (i = TRUE) THEN
        EXECUTE TASK task_load_artist;
    END IF;
    LET j BOOLEAN DEFAULT IFF(EXISTS (SELECT * FROM "stage_track"), TRUE, FALSE);
    -- Check if any rows exist in the subquery
    IF (j = TRUE) THEN
        EXECUTE TASK task_load_track;
    END IF;
    RETURN 'Error 1001: No data, stopped';
END;
$$;

CREATE OR REPLACE PROCEDURE stored_load_artist_from_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO "artist" AS tgt
    USING (
        SELECT 
            value:id::STRING AS artist_id,
            value:artist::STRING AS name,
            value:genres::ARRAY AS genres,
            value:popularity::NUMBER AS popularity,
            value:followers.total::NUMBER AS followers,
            value:external_urls.spotify::STRING AS external_url,
            value AS metadata
        FROM "stage_artist",
             LATERAL FLATTEN(input => raw_data)
    ) AS src
    ON tgt.artist_id = src.artist_id
    WHEN MATCHED THEN UPDATE SET
        name = src.name,
        genres = src.genres,
        popularity = src.popularity,
        followers = src.followers,
        external_url = src.external_url,
        metadata = src.metadata,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        artist_id, name, genres, popularity, followers, external_url, metadata, updated_at
    ) VALUES (
        src.artist_id, src.name, src.genres, src.popularity, src.followers, src.external_url, src.metadata, CURRENT_TIMESTAMP()
    );

    RETURN 'Artist data loaded successfully';
END;
$$;

CREATE OR REPLACE PROCEDURE stored_load_tracks_from_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO "track" AS tgt
USING (
    SELECT 
        track.value:track_id::STRING AS track_id,
        track.value:track_name::STRING AS track_name,
        artist.value:artist::STRING AS artist_id,
        track.value AS metadata
    FROM "stage_track" AS s,
         LATERAL FLATTEN(input => s.raw_data) AS artist,  -- artist level
         LATERAL FLATTEN(input => artist.value:albums) AS album,  -- albums
         LATERAL FLATTEN(input => album.value:tracks) AS track  -- tracks 
) AS src
ON tgt.track_id = src.track_id

WHEN MATCHED THEN UPDATE SET
    track_name = src.track_name,
    artist_id = src.artist_id,
    metadata = src.metadata,
    updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    track_id, track_name, artist_id, metadata, updated_at
) VALUES (
    src.track_id, src.track_name, src.artist_id, src.metadata, CURRENT_TIMESTAMP()
);

RETURN 'Track data loaded successfully';

END;
$$;

CREATE OR REPLACE PROCEDURE stored_proc_clear_artist_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM "stage_artist";
    RETURN 'Completed transfer data';
END;
$$;
CREATE OR REPLACE PROCEDURE stored_proc_clear_track_stage()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM "stage_track";
    RETURN 'Completed transfer data';
END;
$$;

CREATE OR REPLACE TASK task_copy_stage
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 MINUTE'
AS CALL stored_copy_stage();

CREATE OR REPLACE TASK task_is_empty_stage
WAREHOUSE = COMPUTE_WH
SCHEDULE = '30 MINUTE'
AS CALL stored_proc_is_empty_stage();

CREATE OR REPLACE TASK task_load_artist 
WAREHOUSE = COMPUTE_WH
AS CALL stored_load_artist_from_stage();

CREATE OR REPLACE TASK task_load_track
WAREHOUSE = COMPUTE_WH
AS CALL stored_load_track_from_stage();

CREATE OR REPLACE TASK task_clear_artist
WAREHOUSE = COMPUTE_WH
AFTER task_load_artist
AS CALL stored_proc_clear_artist_stage();

CREATE OR REPLACE TASK task_clear_track
WAREHOUSE = COMPUTE_WH
AFTER task_load_track
AS CALL stored_proc_clear_track_stage();

-- Stop and start task process (automatically)
ALTER TASK task_copy_stage SUSPEND;
ALTER TASK task_is_empty_stage SUSPEND;
ALTER TASK task_load_artist SUSPEND;
ALTER TASK task_load_track SUSPEND;
ALTER TASK task_clear_artist SUSPEND;
ALTER TASK task_clear_track SUSPEND;

ALTER TASK task_copy_stage RESUME;
ALTER TASK task_clear_artist RESUME;
ALTER TASK task_clear_track RESUME;
-- ALTER TASK task_load_artist RESUME;
-- ALTER TASK task_load_track RESUME;
ALTER TASK task_is_empty_stage RESUME;

-- For debugging:
-- DELETE FROM "artist";
-- CALL load_artist_data();
-- CALL load_tracks_from_stage();

-- Deplecated

-- CREATE OR REPLACE TABLE "user" (
--     user_id STRING PRIMARY KEY,
--     user_name STRING,
--     user_email STRING,
--     preferences VARIANT,         -- e.g., preferred genres, languages, etc.
--     metadata VARIANT,            -- optional nested user profile data
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
-- );
-- -- CREATE OR REPLACE TABLE "genre" (
--     genre_id STRING PRIMARY KEY,
--     name STRING,
--     description STRING,
--     related_genres VARIANT,      -- e.g., ["pop", "dance pop", "synthpop"]
--     metadata VARIANT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
-- );

-- CREATE OR REPLACE TABLE "user_favorite" (
--     user_id STRING REFERENCES "user"(user_id),
--     track_id STRING REFERENCES "track"(track_id),
--     liked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
--     context VARIANT,             -- e.g., source: 'playlist', mood: 'happy', location: 'NYC'
--     PRIMARY KEY (user_id, track_id)
-- );
