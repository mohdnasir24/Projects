import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE
        IF NOT EXISTS staging_events
        (
                artist        VARCHAR      ,
                auth          VARCHAR NOT NULL,
                firstname     VARCHAR ,
                gender        VARCHAR ,
                iteminsession INT   NOT NULL ,
                lastname      VARCHAR ,
                LENGTH        FLOAT   ,
                level         VARCHAR NOT NULL ,
                location      VARCHAR         ,
                method        VARCHAR NOT NULL,
                page          VARCHAR NOT NULL,
                registration  BIGINT  ,
                sessionid     INT NOT NULL,
                song          VARCHAR ,
                status        INT NOT NULL,
                ts            BIGINT NOT NULL,
                userAgent    VARCHAR ,
                userId       INT
        )
        diststyle auto;""")

staging_songs_table_create = ("""CREATE TABLE
        IF NOT EXISTS staging_songs
        (
                num_songs        INT              ,
                artist_id        VARCHAR          ,
                artist_latitude  FLOAT            ,
                artist_longitude FLOAT            ,
                artist_location  VARCHAR          ,
                artist_name      VARCHAR          ,
                song_id           VARCHAR NOT NULL ,
                title            VARCHAR NOT NULL ,
                duration         FLOAT NOT NULL   ,
                YEAR             INT NOT NULL     
        )
        diststyle auto;""")

songplay_table_create = ("""CREATE TABLE
        IF NOT EXISTS songplays
        (
                songplay_id INT IDENTITY(0,1) PRIMARY KEY ,
                start_time TIME NOT NULL       ,
                user_id    INT      ,
                level      VARCHAR             ,
                song_id    VARCHAR             ,
                artist_id  VARCHAR             ,
                session_id VARCHAR NOT NULL    ,
                location   VARCHAR     ,
                user_agent VARCHAR NOT NULL
        )
        diststyle even;""")

user_table_create = ("""CREATE TABLE
        IF NOT EXISTS users
        (
                user_id    INT PRIMARY KEY ,
                first_name VARCHAR NOT NULL,
                last_name  VARCHAR         ,
                gender     VARCHAR         ,
                level      VARCHAR
        )diststyle all;""")

song_table_create = ("""CREATE TABLE
        IF NOT EXISTS songs
        (
                song_id   VARCHAR PRIMARY KEY,
                title     VARCHAR NOT NULL   ,
                artist_id VARCHAR NOT NULL   ,
                YEAR      INT NOT NULL       ,
                duration  FLOAT NOT NULL
        )diststyle all;""")

artist_table_create = ("""CREATE TABLE
        IF NOT EXISTS artists
        (
                artist_id VARCHAR PRIMARY KEY,
                name      VARCHAR NOT NULL   ,
                location  VARCHAR    ,
                latitude  FLOAT      ,
                longitude FLOAT 
        )diststyle all;""")


time_table_create = ("""CREATE TABLE
        IF NOT EXISTS TIME
        (
                start_time TIME PRIMARY KEY,
                hour       INT NOT NULL    ,
                DAY        INT NOT NULL    ,
                week       INT NOT NULL    ,
                MONTH      INT NOT NULL    ,
                YEAR       INT NOT NULL    ,
                weekday    INT NOT NULL
        )diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events from {}
                        credentials 'aws_iam_role={}'
                        ';' 
                        json as {}
                        compupdate off region 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                        copy staging_songs from {}
                        credentials 'aws_iam_role={}'
                        ';' format as json 'auto' compupdate off region 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO
        songplays
        (
                start_time,
                user_id   ,
                level     ,
                song_id   ,
                artist_id ,
                session_id,
                location  ,
                user_agent
        )
        SELECT DISTINCT cast(date_add('ms',se.ts,'1970-01-01') as time) as start_time,
                se.userid   ,
                se.level     ,
                ss.song_id   ,
                ss.artist_id ,
                se.sessionid,
                ss.artist_location  ,
                se.useragent
                from staging_events se,
                staging_songs   ss
                WHERE
                ss.song_id IS NOT NULL
                AND     ss.artist_id IS NOT NULL
                AND     se.page='NextSong'
                AND     ss.title=se.song
                AND     ss.artist_name=se.artist
                AND     ss.duration=se.length;""")

user_table_insert = ("""INSERT INTO
        users
        (
                user_id    ,
                first_name ,
                last_name  ,
                gender,
                level
        )
        SELECT DISTINCT se.userid,
                se.firstname,
                se.lastname,
                se.gender,
                se.level
                from staging_events se
                left outer join users u 
                on se.userid=u.user_id
                where u.user_id is null
                and se.userid is not null;""")

song_table_insert = ("""INSERT INTO
        songs
        (
                song_id   ,
                title     ,
                artist_id ,
                YEAR      ,
                duration
        )
        SELECT  DISTINCT ss.song_id,
                ss.title,
                ss.artist_id,
                ss.year,
                ss.duration 
                from staging_songs ss
                left outer join songs s
                on ss.song_id=s.song_id
                where s.song_id is null
                and ss.song_id is not null;""")

artist_table_insert = ("""INSERT INTO
        artists
        (
                artist_id ,
                name      ,
                location  ,
                latitude  ,
                longitude
        )
        SELECT  DISTINCT ss.artist_id,
                ss.artist_name      ,
                ss.artist_location  ,
                ss.artist_latitude  ,
                ss.artist_longitude 
                from staging_songs ss
                left outer join artists a
                on ss.artist_id=a.artist_id
                where a.artist_id is null
                and ss.artist_id is not null;""")

time_table_insert = ("""INSERT INTO
        TIME
        (
                start_time ,
                hour       ,
                DAY        ,
                week       ,
                MONTH      ,
                YEAR       ,
                weekday
        )
        select DISTINCT cast(date_add('ms',se.ts,'1970-01-01') as time) as start_time
        ,date_part('h',date_add('ms',se.ts,'1970-01-01')) as hour
        ,date_part('d',date_add('ms',se.ts,'1970-01-01')) as day
        ,date_part('w',date_add('ms',se.ts,'1970-01-01')) as week
        ,date_part('mon',date_add('ms',se.ts,'1970-01-01')) as month
        ,date_part('year',date_add('ms',se.ts,'1970-01-01')) as year
        ,date_part('weekday',date_add('ms',se.ts,'1970-01-01')) as weekday from staging_events se
        left outer join time t
        on cast(date_add('ms',se.ts,'1970-01-01') as time)=t.start_time
        where t.start_time is null
        and se.ts is not null;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
