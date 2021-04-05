# Project Data Warehouse
## Project Overview

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

## Schema

### Fact Tables

staging_events

    artist,auth,first_name,gender,item_session,last_name,length,level,location,method,page,registration,session_id,song,status,ts,user_agent,user_id

    
staging_songs

    num_songs,artist_id,artist_latitude,artist_longitude,artist_location,artist_name,song_id,title,duration,year


### Dimension Tables

users
    
    user_id,first_name,last_name,gender,level
    
songs

    song_id,title,artist_id,year,duration
    
artists

    artist_id,name,location,lattitude,longitude
    
time

    start_time,hour,day,week,month,year,weekday
    
    
## Run the Process
 
### Pre-Req

    1. Json files in S3
    2. redshift    
 
### Create tables
 
     create_table.py
 
### Load
 
     etl.py

