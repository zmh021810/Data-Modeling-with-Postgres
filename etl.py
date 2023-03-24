import datetime
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    
    """
        Description: This function is used to load all files in the filepath (data/song_data).
        also it changes the data format of column 'ts' to timestamp. then it generates the song and artist tables.
        Arguments:
            filepath: file path of the song data. 
        Returns:
            None
    """

    
    
    
    
    
    
    
    # open song file
    json_table = pd.read_json(filepath, lines=True)

    # insert song record
    # choose the five parameters based on the json_table request from the df dataframe and change it to the list format
    song_data =json_table[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0] 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    # choose the five parameters based on the json_table request from the df dataframe and change it to the list format
    artist_data = json_table[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0] 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Description: This function is used to load all files in the filepath (data/log_data).
        also it changes the data format of column 'ts' to timestamp. then it generates the songsplay tables.
        Arguments:
            filepath: file path of the log data. 
        Returns:
            None
    """
    

    
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_NextSong = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t=pd.to_datetime(df_NextSong['ts']/1000)
    
    # insert time data records
    t_start_time=t.apply(lambda x:str(x))
    t_hour=t.dt.hour
    t_day=t.dt.day
    t_week=t.dt.week
    t_month=t.dt.month
    t_year=t.dt.year
    t_weekday=t.dt.weekday

    time_df=pd.concat([t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday], axis=1)
    time_df.columns = ['start_time','hour', 'day', 'week', 'month', 'year', 'weekday']
    #time_df.head(5)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

        
        
        
        
        
        
        
        
            
    # load user table
    #df=df.loc[df['userId']!= ""]
    try:
        df_move = df_NextSong.loc[df_NextSong['userId']!=""]
        user_df = df_move[['userId', 'firstName', 'lastName', 'gender', 'level']]
        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except:
        pass
    
        
        
        
        
    # insert songplay records
    #df_NextSong = df_NextSong.loc[df_NextSong['userId']!=""]
    for index, row in df_NextSong.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

                 
            
            
        # insert songplay record
        songplay_data = (datetime.datetime.fromtimestamp(row.ts/1000), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Description: This function is used to load all files in the filepath (data/song_data).
        Arguments:
            filepath: file path of the song data. 
        Returns:
            None
    """
    

    
    
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()