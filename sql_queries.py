import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ENDPOINT=config.get("CLUSTER","HOST")
DWH_DB=config.get("CLUSTER","DB_NAME")
DWH_DB_USER=config.get("CLUSTER","DB_USER")
DWH_DB_PASSWOED=config.get("CLUSTER","DB_PASSWORD")
DWH_PORT=config.get("CLUSTER","DB_PORT")
LOG_DATA_PATH=config.get("S3","LOG_DATA")
SONG_DATA_PATH=config.get("S3","LOG_JSONPATH")
DWH_ARN=config.get("IAM_ROLE","ARN")
LOG_DATA_JSONPATH = config.get("S3","LOG_JSONPATH")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER SORTKEY DISTKEY,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    );
""")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
        num_songs                  INTEGER
        ,artist_id                VARCHAR(255)
        ,artist_latitude          FLOAT
        ,artist_longitude         FLOAT
        ,artist_location          VARCHAR(255)
        ,artist_name              VARCHAR(255)
        ,song_id                  VARCHAR(255)
        ,title                    VARCHAR
        ,duration                 FLOAT
        ,year                     INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
        songplay_id    INTEGER IDENTITY(0,1)                   PRIMARY KEY
        ,start_time    TIMESTAMP               NOT NULL         sortkey
        ,user_id       VARCHAR(255)            NOT NULL
        ,level         VARCHAR(8)
        ,song_id        VARCHAR(255) 
        ,artist_id     VARCHAR(255)
        ,session_id    INTEGER
        ,location      VARCHAR(255)
        ,user_agent    TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id        VARCHAR(255) PRIMARY KEY
                                ,title           VARCHAR(255)
                                ,artist_id       VARCHAR(255)
                                ,year            INTEGER
                                ,DURATION        FLOAT);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR(255)PRIMARY KEY 
,name VARCHAR,title VARCHAR(255),artist_id VARCHAR(255),year INTEGER,duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(255)PRIMARY KEY  ,name VARCHAR(255),location VARCHAR(255),latitude FLOAT,longitude FLOAT );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP PRIMARY KEY SORTKEY ,name VARCHAR(255),hour INTEGER,day INTEGER,week INTEGER,month TEXT,YEAR INTEGER ,weekday TEXT);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
(user_id,title,first_name,last_name,gender,level)
SELECT DISTINC se.userid,se.firstname,se.lastname,se.gender,se.level
FROM staging_events se WHERE se.userid IS NOT NULL AND  se.page='NextSong';
""")
song_table_insert = ("""
    INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id,ss.title,ss.artist_id,ss.year,ss.duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (artist_id, name, location, lattitude, longitude)
    SELECT  DISTINCT ss.artist_id,ss.artist_name,ss.artist_location,ss.artist_latitude,ss.artist_longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second' AS start_time, 
    EXTRACT (hour from sp.start_time),
    EXTRACT (day from sp.start_time) ,
    EXTRACT (week from sp.start_time),
    TO_CHAR (sp.start_time, 'MONTH'),
    EXTRACT (year from sp.start_time),
    TO_CHAR (sp.start_time, 'DAY')
    FROM songplays sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
