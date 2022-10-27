import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

#########################################################################################################

# CREATE TABLES
print("Creating tables")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR(256),
auth VARCHAR(16),
first_name VARCHAR(256),
gender VARCHAR(256),
item_in_session INTEGER,
last_name VARCHAR(256),
length NUMERIC(10, 4),
level VARCHAR(8),
location VARCHAR(256),
method VARCHAR(6),
page VARCHAR(16),
registration BIGINT,
session_id INTEGER,
song_title VARCHAR(256),
status INTEGER,
timestamp BIGINT,
user_agent VARCHAR(256),
user_id INTEGER
)""")


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs INTEGER,
artist_id VARCHAR(20),
artist_latitude NUMERIC(9, 6),
artist_longitude NUMERIC(9, 6),
artist_location VARCHAR(256),
artist_name VARCHAR(256),
song_id VARCHAR(256),
title VARCHAR(256),
duration NUMERIC(10, 6),
year INTEGER
)""")


####################################################################################################

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(
songplay_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id BIGINT NOT NULL,
level VARCHAR NOT NULL,
song_id VARCHAR,
artist_id VARCHAR, 
session_id INTEGER,
location VARCHAR NOT NULL,
user_agent VARCHAR NOT NULL
)""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(
user_id BIGINT NOT NULL PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR NOT NULL,
level VARCHAR NOT NULL
)""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR NOT NULL PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INT NOT NULL,
duration FLOAT NOT NULL
)""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR NOT NULL PRIMARY KEY,
artist_name VARCHAR NOT NULL,
artist_location VARCHAR NOT NULL,
artist_latitude FLOAT NOT NULL,
artist_longitude FLOAT NOT NULL
)""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP NOT NULL PRIMARY KEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday INTEGER NOT NULL
)""")


###############################################################################################################################################################################################

# STAGING TABLES
print("Staging tables")
log_data = config.get('S3', 'LOG_DATA')
arn = config.get('IAM_ROLE', 'ARN')
log_json = config.get('S3', 'LOG_JSONPATH')
song_data = config.get('S3', 'SONG_DATA')
region = config.get('S3', 'REGION')


staging_events_copy = ("""COPY staging_events
                          FROM {}
                          IAM_ROLE {}
                          JSON {}
                          REGION {};""").format(log_data, arn, log_json, region)


staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         IAM_ROLE {}
                         JSON 'auto'
                         REGION {};""").format(song_data, arn, region)

######################################################################################################################################

# FINAL TABLES
print("Creating final tables\n")

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT timestamp 'epoch' + (se.timestamp/1000 * INTERVAL '1 second') as start_time,
                            se.user_id as user_id,
                            se.level as level,
                            ss.song_id as song_id,
                            ss.artist_id as artist_id,
                            se.session_id as session_id,
                            se.location as location,
                            se.user_agent as user_agent
                            FROM staging_events se
                            LEFT JOIN staging_songs ss ON
                            se.song_title = ss.title AND
                            se.artist = ss.artist_name AND
                            ABS(se.length - ss.duration) < 2
                            WHERE se.page='NextSong'""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id,
                        first_name,
                        last_name,
                        gender,
                        level
                        FROM staging_events
                        WHERE user_id IS NOT NULL""")



song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL""")


artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                          SELECT DISTINCT artist_id,
                          artist_name,
                          artist_location,
                          artist_latitude,
                          artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
                          AND artist_location IS NOT NULL
                          AND artist_latitude IS NOT NULL
                          AND artist_longitude IS NOT NULL""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT start_time,
                        extract(hour from start_time) as hour,
                        extract(day from start_time) as day,
                        extract(week from start_time) as week,
                        extract(month from start_time) as month,
                        extract(year from start_time) as year,
                        extract(dayofweek from start_time) as weekday
                        FROM songplays""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]