import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events_table (
        artist varchar(max),
        auth varchar(10),
        firstName varchar(30),
        gender varchar(1),
        itemInSession integer,
        lastName varchar(50),
        length decimal(15,5),
        level varchar(10),
        location varchar(50),
        method varchar(10),
        page varchar(20),
        registration decimal(15,1),
        sessionId integer,
        song varchar(max),
        status integer,
        ts bigint,
        userAgent varchar(max),
        userId integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs_table (
        num_songs integer,
        artist_id varchar(18),
        artist_latitude varchar(50),
        artist_longitude varchar(50),
        artist_location varchar(max),
        artist_name varchar(max),
        song_id varchar(18),
        title varchar(max),
        duration decimal(15,5),
        year integer
    );

""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint identity(0, 1),
        start_time bigint,
        user_id integer NOT NULL,
        level varchar(10),
        song_id varchar(18),
        artist_id varchar(18),
        session_id integer,
        location varchar(50),
        user_agent varchar(max)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer NOT NULL,
        first_name varchar(30),
        last_name varchar(50),
        gender varchar(1),
        level varchar(10)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar(18) NOT NULL,
        title varchar(max),
        artist_id varchar(18) NOT NULL,
        year integer NOT NULL,
        duration decimal(15,5)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(18) NOT NULL,
        name varchar(max),
        location varchar(max),
        lattitude varchar(50),
        longitude varchar(50)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint NOT NULL,
        hour integer NOT NULL,
        day integer NOT NULL,
        week integer NOT NULL,
        month integer NOT NULL,
        year integer NOT NULL,
        weekday integer NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy {} from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2';
""").format('staging_events_table',config['S3']['log_data'], config['IAM_ROLE']['ARN'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
    copy {} from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format('staging_songs_table',config['S3']['song_data'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent   
    )
    SELECT DISTINCT
        stg.ts AS start_time,
        stg.userId,
        stg.level,
        stg2.song_id,
        stg2.artist_id,
        stg.sessionId,
        stg.location,
        stg.userAgent
    FROM staging_events_table stg
    LEFT JOIN staging_songs_table stg2
        ON stg.artist = stg2.artist_name
        AND stg.song = stg2.title
    WHERE stg.userId IS NOT NULL
    AND stg.page = 'NextSong'
    ;
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        stg.userId AS user_id,
        stg.firstName AS first_name,
        stg.lastName AS last_name,
        stg.gender,
        stg.level
    FROM staging_events_table stg
    LEFT JOIN users u
        ON stg.userId = u.user_id
    WHERE stg.userID IS NOT NULL
    AND stg.page = 'NextSong'
    AND u.user_id IS NULL
    ;
""")

song_table_insert = ("""
    INSERT INTO SONGS
    SELECT 
        stg.song_id,
        stg.title,
        stg.artist_id,
        stg.year,
        stg.duration
    FROM staging_songs_table stg
    LEFT JOIN songs s
        ON stg.song_id = s.song_id
    WHERE stg.song_id IS NOT NULL
    AND s.song_id IS NULL
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT
        stg.artist_id,
        stg.artist_name AS name,
        stg.artist_location AS location,
        stg.artist_latitude AS lattitude,
        stg.artist_longitude AS longitude
    FROM staging_songs_table stg
    LEFT JOIN artists a
        ON stg.artist_id = a.artist_id
    WHERE stg.artist_id IS NOT NULL
    AND a.artist_id IS NULL
    ;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
        stg.ts AS start_time,
        extract(hour from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS hour,
        extract(day from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS day,
        extract(week from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS week,
        extract(month from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS month,
        extract(year from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS year,
        extract(weekday from TIMESTAMP 'epoch' + stg.ts/1000 *INTERVAL '1 second') AS weekday
    FROM staging_events_table stg
    LEFT JOIN time t
        ON stg.ts = t.start_time
    WHERE stg.ts IS NOT NULL
    AND t.start_time IS NULL
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
