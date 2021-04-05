import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# parameter 
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_PATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES - STAGE
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist text,
                                auth text,
                                first_name text,
                                gender char(1),
                                item_session integer,
                                last_name text,
                                length numeric,
                                level text,
                                location text,
                                method text,
                                page text,
                                registration numeric,
                                session_id integer,
                                song text,
                                status integer,
                                ts bigint,
                                user_agent text,
                                user_id integer )""")


staging_songs_table_create =  ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                num_songs integer,
                                artist_id varchar(MAX),
                                artist_latitude numeric,
                                artist_longitude numeric,
                                artist_location varchar(MAX),
                                artist_name varchar(MAX),
                                song_id varchar(MAX),
                                title varchar(MAX),
                                duration numeric,
                                year integer)""")

# CREATE TABLES - analytics


songplay_table_create =  ("""CREATE TABLE songplay(
                            songplay_id int IDENTITY(1,1) PRIMARY KEY sortkey,
                            start_time timestamp,
                            user_id integer NOT NULL,
                            level text,
                            song_id text,
                            artist_id text,
                            session_id integer,
                            location text,
                            user_agent text,
                            duration numeric)""")

#sort by user_id and distribution by user_id 

user_table_create = ("""
create table users (
user_id integer not null PRIMARY KEY sortkey distkey, 
first_name text not null, 
last_name text not null, 
gender text not null, 
level text not null );
""")

#sort by song_id and distribution all
song_table_create = ("""
create table songs (
song_id varchar(max)  PRIMARY KEY not null sortkey, 
title varchar(max) not null, 
artist_id varchar(max)  not null,
year integer not null, 
duration numeric not null ) diststyle all;
""")

#sort by artist_id and distribution all
artist_table_create = ("""
create table artists (
artist_id varchar(max) PRIMARY KEY not null sortkey , 
name varchar(max) not null, 
location varchar(max) not null, 
lattitude varchar(max), 
longitude varchar(max)) diststyle all; 
""")

#sort by start_time and distribution all
time_table_create = ("""
create table time ( 
start_time timestamp PRIMARY KEY not null sortkey , 
hour integer not null, 
day integer not null, 
week integer not null, 
month integer not null, 
year integer not null, 
weekday text not null ) diststyle all; 
""")

# STAGING TABLES copy


staging_events_copy = (f"""copy staging_events 
                          from {LOG_DATA}
                          iam_role {IAM_ROLE}
                          json {LOG_PATH}; """)

staging_songs_copy = (f"""copy staging_songs 
                          from {SONG_DATA} 
                          iam_role {IAM_ROLE}
                          json 'auto'; """)

# FINAL TABLES insert
#INSERT FROM STAGING TABLES

songplay_table_insert = ("""insert into songplay(start_time, user_id, level,song_id, artist_id, session_id, location, user_agent, duration) 
    select GETDATE(), 
    se.user_id, 
    se.level,
    ss.song_id, 
    ss.artist_id,
    se.session_id, 
    se.location, 
    se.user_agent,
    se.duration
    from staging_events se 
    join staging_songs ss on (ss.title = se.song) 
    AND se.artist = ss.artist_name
    AND se.page = 'NextSong';  
""")


user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level)
select  distinct se.user_id,
        se.first_name,
        se.last_name,
        se.gender,
        se.level
        from staging_events se
        where page = 'NextSong'
        and user_id IS NOT NULL;
""")

song_table_insert = ("""insert into songs (song_id,title,artist_id,year,duration) 
select distinct ss.song_id, 
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
from staging_songs ss
where song_id IS NOT NULL;
""") 

artist_table_insert = ("""insert into artists (artist_id,name,location,lattitude,longitude) 
select
    distinct artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude    
from staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""insert into time
select distinct
       timestamp 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       extract(HOUR FROM start_time) AS hour,
       extract(DAY FROM start_time) AS day,
       extract(WEEKS FROM start_time) AS week,
       extract(MONTH FROM start_time) AS month,
       extract(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

