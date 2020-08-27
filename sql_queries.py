import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE events_staging 
(
  artist            VARCHAR,
  auth              VARCHAR(25),
  firstName         VARCHAR(50),
  gender            CHAR,
  itemInSession     INTEGER,
  lastName         VARCHAR(50),
  length            DECIMAL,
  level             VARCHAR(10),
  location          VARCHAR,
  method            VARCHAR(10),
  page              VARCHAR,
  registration      DECIMAL,
  sessionId         INTEGER,
  song              VARCHAR,
  status            INTEGER,
  ts                TIMESTAMP,
  userAgent         VARCHAR,
  userId            INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE songs_staging 
(
  num_songs         INTEGER,
  artist_id         VARCHAR NOT NULL,
  artist_latitude   DECIMAL,
  artist_longitude  DECIMAL,
  artist_location   VARCHAR,
  artist_name       VARCHAR,
  song_id           VARCHAR NOT NULL,
  title             VARCHAR,
  duration          DECIMAL,
  year              INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
  songplay_id       INTEGER IDENTITY(1,1) PRIMARY KEY,
  start_time        TIMESTAMP DISTKEY SORTKEY REFERENCES time(start_time),
  user_id           INTEGER NOT NULL REFERENCES users(user_id),
  level             VARCHAR(10) NOT NULL,
  song_id           VARCHAR REFERENCES songs(song_id),
  artist_id         VARCHAR REFERENCES artists(artist_id),
  session_id        INTEGER NOT NULL,
  location          VARCHAR,
  user_agent        VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users
(
  user_id           INTEGER PRIMARY KEY SORTKEY,
  first_name        VARCHAR(50),
  last_name         VARCHAR(50),
  gender            CHAR NOT NULL,
  level             VARCHAR(10) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs
(
  song_id           VARCHAR PRIMARY KEY NOT NULL SORTKEY,
  title             VARCHAR,
  num_songs         INTEGER,
  year              INTEGER,
  duration          DECIMAL
);
""")

artist_table_create = ("""
CREATE TABLE artists
(
  artist_id         VARCHAR PRIMARY KEY NOT NULL SORTKEY,
  artist_name       VARCHAR,
  artist_location   VARCHAR,
  artist_latitude   DECIMAL,
  artist_longitude  DECIMAL
)
""")

time_table_create = ("""
CREATE TABLE time
(
  start_time      TIMESTAMP PRIMARY KEY NOT NULL SORTKEY,
  hour            INTEGER NOT NULL,
  day             INTEGER NOT NULL,
  week            INTEGER NOT NULL,
  month           INTEGER NOT NULL,
  year            INTEGER NOT NULL,
  weekday         INTEGER NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy "events_staging" from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy "songs_staging" from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
( 
SELECT DISTINCT userId AS user_id, firstName AS first_name, 
lastName AS last_name, gender, level
FROM events_staging
WHERE page='NextSong'
AND user_id IS NOT NULL
)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, num_songs, year, duration)
( 
SELECT DISTINCT song_id, title, num_songs, year, duration
FROM songs_staging
WHERE song_id IS NOT NULL
)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
( 
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM songs_staging
WHERE artist_id IS NOT NULL
)
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, 
artist_id, session_id, location, user_agent)
(
SELECT e.ts AS start_time, e.userId, e.level, s.song_id, 
s.artist_id, e.sessionId, e.location, e.userAgent
FROM events_staging AS e
JOIN songs_staging AS s ON 
(
    e.song = s.title AND e.artist = s.artist_name
)
WHERE e.page='NextSong'
)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
( 
SELECT start_time AS start_time, EXTRACT(hr FROM start_time) AS hour, 
EXTRACT(d FROM start_time) AS day, 
EXTRACT(w FROM start_time) AS week, 
EXTRACT(mon FROM start_time) AS month, 
EXTRACT(y FROM start_time) AS year, 
EXTRACT(dayofweek FROM start_time) AS weekday
FROM songplays
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
