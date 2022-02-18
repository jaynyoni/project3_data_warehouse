import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
    (artist           VARCHAR,
     auth             VARCHAR,
     firstName        VARCHAR,
     gender           VARCHAR,
     itemInSession    INT,
     lastName         VARCHAR,
     length           FLOAT,
     level            VARCHAR,
     location         VARCHAR,
     method           VARCHAR,
     page             VARCHAR,
     registration     FLOAT,
     sessionId        INT,
     song             VARCHAR,
     status           INT,
     ts               TIMESTAMP,
     userAgent        VARCHAR,
     userId           INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs        INT,
     artist_id        VARCHAR,
     artist_latitude  FLOAT,
     artist_longitude FLOAT,
     artist_location  VARCHAR,
     artist_name      VARCHAR,
     song_id          VARCHAR,
     title            VARCHAR,
     duration         FLOAT,
     year             INT
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
    (songplay_id      INT IDENTITY(0,1) PRIMARY KEY, 
    start_time        TIMESTAMP SORTKEY, 
    user_id           INT NOT NULL, 
    level             VARCHAR, 
    song_id           VARCHAR NOT NULL DISTKEY, 
    artist_id         VARCHAR NOT NULL, 
    session_id        INT NOT NULL, 
    location          VARCHAR, 
    user_agent        VARCHAR)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
    (user_id          INT PRIMARY KEY, 
    first_name        VARCHAR, 
    last_name         VARCHAR, 
    gender            VARCHAR, 
    level             VARCHAR) DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
    (song_id          VARCHAR SORTKEY DISTKEY PRIMARY KEY, 
    title             VARCHAR, 
    artist_id         VARCHAR NOT NULL, 
    year              INT, 
    duration          FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
    (artist_id        VARCHAR SORTKEY PRIMARY KEY,
     name             VARCHAR, 
     location         VARCHAR, 
     latitude         FLOAT, 
     longitude        FLOAT) DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
    (start_time       TIMESTAMP SORTKEY PRIMARY KEY,
     hour             INT, 
     day              INT, 
     week             INT, 
     month            INT, 
     year             INT, 
     weekday          VARCHAR) DISTSTYLE ALL
""")

# STAGING TABLES

staging_events_copy = (f"""
copy staging_events 
    from {LOG_DATA}
    iam_role '{IAM_ROLE}'
    region 'us-west-2'
    compupdate off statupdate off
    format as JSON {LOG_JSONPATH}
    timeformat as 'epochmillisecs';
""")

staging_songs_copy = (f"""
copy staging_songs 
    from {SONG_DATA}
    iam_role '{IAM_ROLE}'
    region 'us-west-2'
    compupdate off statupdate off
    format as JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
select
    distinct staging_events.ts as start_time,
    staging_events.user_id,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.session_id,
    staging_events.location,
    staging_events.user_agent
from staging_events
inner join staging_songs on
    staging_events.artist = staging_songs.artist_name and
    staging_events.song = staging_songs.title
where staging_events.page = 'NextSong'
""")

user_table_insert = ("""
insert into users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
select
    distinct user_id,
    staging_events.first_name,
    staging_events.last_name,
    staging_events.gender,
    staging_events.level
from staging_events
where staging_events.page = 'NextSong' and
user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
insert into songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
select
    distinct staging_songs.song_id,
    staging_songs.title,
    staging_songs.artist_id,
    staging_songs.year,
    staging_songs.duration
from staging_songs
where staging_songs.song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
insert into artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
select
    distinct staging_songs.artist_id,
    staging_songs.artist_name as name,
    staging_songs.artist_location as location,
    staging_songs.artist_latitude as latitude,
    staging_songs.artist_longitude as longitude
from staging_songs
where staging_songs.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
insert into time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
select
    distinct staging_events.ts as start_time,
    extract (hour from staging_events.ts) as hour,
    extract (day from staging_events.ts) as day,
    extract (week from staging_events.ts) as week,
    extract (month from staging_events.ts) as month,
    extract (year from staging_events.ts) as year,
    extract (weekday from staging_events.ts) as weekday
from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
