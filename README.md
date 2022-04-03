
# PROJECT-4: Data Lake
## Overview
- This project processes the data of a music streaming startup, Sparkify. As with previous projects, a dataset is a collection of files in JSON format and they are stored in an AWS S3 bucket.
### 1.Fact Table
- songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### 2.Dimension Tables
- users: user_id, first_name, last_name, gender, level
- songs: columns: song_id, title, artist_id, year, duration
- artists: columns: artist_id, name, location, latitude, longitude
- time: columns: start_time, hour, day, week, month, year, weekday

### In put
- Data files of music streaming startup company
  - Song data: s3://udacity-dend/song_data
  - Log data: s3://udacity-dend/log_data
### How to use
- Create aws account, get accesskey (KEY) and secret (SECRET) information and fill in dl.cfg.
- Create udacity-dend-output-1003 folder in your S3 bucket.
- Write python functions for each table Fact, Dim, use Spark to process data.
- On a terminal window run the command python etl.py.
### Out put
- Data is split into fact, dim tables and saved in aws s3 with parquet format.
    - Fact table : songplays_table.
    - Dim tables: song_table, artist_table, user_table, stimes_table.
