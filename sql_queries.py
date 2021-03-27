import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS
staging_events ( artist varchar,auth varchar, firstname varchar, gender varchar, iteminsession int, lastname varchar, length float, level varchar, location varchar, method varchar, page varchar, registration varchar, sessionid int SORTKEY DISTKEY, song varchar, status int, ts bigint, useragent text, userid int );
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS
staging_songs (num_songs int, artist_id varchar SORTKEY DISTKEY,artist_latitude float, artist_longitude float, artist_location varchar, artist_name varchar, song_id varchar, title text, duration float, year int );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS
songplays (songplay_id INT IDENTITY(0, 1) PRIMARY KEY, start_time time NOT NULL, user_id int , level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent text);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS
users (user_id int PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS
songs (song_id varchar PRIMARY KEY NOT NULL, title text, artist_id varchar NOT NULL, year int, duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS
artists (artist_id varchar PRIMARY KEY NOT NULL, name varchar, location varchar, latitude float, longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS
time (start_time time PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from 's3://udacity-dend/log_data'
     credentials 'aws_iam_role={}'
     timeformat as 'epochmillisecs'
     format as json 's3://udacity-dend/log_json_path.json'
     compupdate off region 'us-west-2';
""").format(DWH_ARN)

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
     credentials 'aws_iam_role={}'
     json 'auto' 
     compupdate off region 'us-west-2';
""").format(DWH_ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays(start_time,user_id,level,song_id, artist_id, session_id , location, user_agent)
SELECT e.ts2,e.userid,e.level,s.song_id,s.artist_id, e.sessionid,e.location,e.useragent FROM staging_songs s, staging_events e
     WHERE e.song = s.title
       AND e.page = 'NextSong';
""")

user_table_insert = (""" INSERT INTO users(user_id, first_name, last_name , gender, level)
                        SELECT DISTINCT e.userid,e.firstname,e.lastname,e.gender,e.level FROM staging_events e
                        WHERE  e.page = 'NextSong';
""")

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id , year, duration)
                        SELECT DISTINCT s.song_id,s.title,s.artist_id,s.year,s.duration FROM staging_songs s;
                        """)

artist_table_insert = (""" INSERT INTO artists(artist_id, name , location , latitude , longitude )
                        SELECT DISTINCT s.artist_id,s.artist_name,s.artist_location,s.artist_latitude,s.artist_longitude FROM staging_songs s;
                        """)

time_table_insert = (""" INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT e.ts2,EXTRACT(HOUR FROM e.ts2),EXTRACT(DAY FROM e.ts2),EXTRACT(WEEK FROM e.ts2),EXTRACT(MONTH FROM e.ts2),EXTRACT(YEAR FROM e.ts2),EXTRACT(DOW FROM e.ts2) FROM staging_events e
                        WHERE  e.page = 'NextSong';
""")
#other method  for extracting timespots from timestamp-->https://stackoverflow.com/questions/59911560/how-can-i-extract-a-weekday-from-a-timestamp-in-postgresql

# ANALYTICAL QUERIES

#anal_query_1 =(""" SELECT * FROM songplays LIMIT 10
#""")

#anal_query_2 = (""" SELECT * FROM time LIMIT 10
#""")

anal_query_3  = (""" SELECT songs.title, artists.name 
                   FROM songs
                   INNER JOIN artists ON songs.artist_id = artists.artist_id WHERE songs.duration <100 LIMIT 10
""")
#Convert bigint to time by first creating a new column and droping the previous one
transform_query_1 = ("""    ALTER TABLE staging_events
ADD ts2 timestamp;""")
transform_query_2 = ("""update staging_events
   set ts2 = TO_TIMESTAMP(ts::text,'YYYYMMDD HH:MI:SS')::timestamp 
   at time zone 'UTC' at time zone 'PST';""")

transform_query_3 = ("""ALTER TABLE staging_events
DROP COLUMN ts;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytical_queries = [anal_query_3]
transform_queries = [transform_query_1,transform_query_2,transform_query_3]

