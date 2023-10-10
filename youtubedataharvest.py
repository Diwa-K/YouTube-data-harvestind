import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import sqlite3

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB connection
url = "mongodb+srv://Diwakar:<password>@cluster0.co4t0ed.mongodb.net/"
client = MongoClient(url, server_api=ServerApi('1'))

db = client["youtube_db"]  #  database name
collection = db["channel_data"]  # collection name

#  YouTube API key
api_key = 'Youtube API'

# function to get channel stats
def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = dict(channel_name=response['items'][0]['snippet']['title'],
                channel_Id=response['items'][0]['id'],
                subscribers=response['items'][0]['statistics']['subscriberCount'],
                views=response['items'][0]['statistics']['viewCount'],
                videos_count=response['items'][0]['statistics']['videoCount'],
                channel_description=response['items'][0]['snippet']['description'],
                playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return data

#  function to get video data
def get_video_data(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id
    )
    response = request.execute()
    videos_ids = []
    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
        response = request.execute()
        for i in range(len(response['items'])):
            videos_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return videos_ids

def get_video_details(youtube, videos_ids, channel_id):
    all_videos_stats = []
    channel_data = get_channel_stats(youtube, channel_id)  
    
    for i in range(0, len(videos_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(videos_ids[i:i+50])
        )
        response = request.execute()
        
        for video in response['items']:
            duration = video['contentDetails']['duration']
            duration_seconds = convert_YouTube_duration_to_seconds(duration)
            
            video_stats = dict(
                ChannelName=channel_data['channel_name'],  
                Title=video['snippet']['title'],
                video_ID=video['id'],
                video_description=video['snippet']['description'],
                Published_date=video['snippet']['publishedAt'],
                views=video['statistics']['viewCount'],
                likes=video['statistics'].get('likeCount'),
                dislikes=video['statistics'].get('dislikeCount'),
                comments=video['statistics'].get('commentCount'),
                favorite=video['statistics']['favoriteCount'],
                thumbnails=video['snippet']['thumbnails']['default']['url'],
                duration=duration,
                duration_seconds=duration_seconds  
            )
            
            all_videos_stats.append(video_stats)
    
    return all_videos_stats

def convert_YouTube_duration_to_seconds(duration):
    days = 0
    hours = 0
    minutes = 0
    seconds = 0

    # Split the duration string into components
    time_components = duration.split('T')

    
    if len(time_components) > 1:
        
        time_part = time_components[1]

        
        if 'D' in time_part:
            days_str, time_part = time_part.split('D')
            days = int(days_str)

        if 'H' in time_part:
            hours_str, time_part = time_part.split('H')
            hours = int(hours_str)

        if 'M' in time_part:
            minutes_str, time_part = time_part.split('M')
            minutes = int(minutes_str)

        if 'S' in time_part:
            seconds_str = time_part.split('S')[0]
            seconds = int(seconds_str)

    
    total_seconds = (days * 24 * 60 * 60) + (hours * 60 * 60) + (minutes * 60) + seconds

    return total_seconds


# Streamlit app
st.title("YouTube Channel Data")

# Initialize channel IDs and current channel index
channel_ids = ["channel_id_1", "channel_id_2", "channel_id_3", "channel_id_4", "channel_id_5",
               "channel_id_6", "channel_id_7", "channel_id_8", "channel_id_9", "channel_id_10"]

# Initialize session state to keep track of app state
if 'channel_index' not in st.session_state:
    st.session_state.channel_index = 0

# Text input for entering channel ID
channel_id = st.text_input("Enter Channel ID", channel_ids[st.session_state.channel_index])

# button to trigger data retrieval and storage in MongoDB and SQLite
if st.button("Get and Store Data"):
    if channel_id:
        youtube = build('youtube', 'v3', developerKey=api_key)
        channel_data = get_channel_stats(youtube, channel_id)
        st.write("Channel Data:")
        st.write(channel_data)

        # Store the data in MongoDB
        collection.insert_one(channel_data)  

        playlist_id = channel_data.get("playlist_id")
        if playlist_id:
            video_ids = get_video_data(youtube, playlist_id)
            video_details = get_video_details(youtube, video_ids, channel_id)
            st.write("Video Details:")
            st.write(video_details)

            # Store video details in MongoDB
            collection.insert_many(video_details)  
        # SQLite Integration
        conn = sqlite3.connect('youtube_db.db')  # Connect to SQLite database
        cursor = conn.cursor()


        # channel_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel_data (
                Channel_Id TEXT ,
                channel_name TEXT,
                subscribers INTEGER,
                views INTEGER,
                videos_count INTEGER,
                channel_description TEXT,
                playlist_id TEXT
            )
        ''')

        
  

        # playlist table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id TEXT,
                channel_id TEXT,
                channel_name TEXT
            )
        ''')

        # video_details table with the channel_name column
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_details (
                channel_name TEXT,
                Title TEXT,
                video_ID TEXT,
                video_description TEXT,
                Published_date TEXT,
                views INTEGER,
                likes INTEGER,
                favorite INTEGER,
                thumbnails TEXT,
                duration_seconds INTEGER
               
            )
        ''')
        
        
       

        # Insert data into the channel_data table
        cursor.execute('''
            INSERT INTO channel_data (channel_Id, channel_name,  subscribers, views, videos_count,
                        channel_description, playlist_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            channel_data.get("channel_Id"),
            channel_data.get("channel_name"),
            channel_data.get("subscribers"),
            channel_data.get("views"),
            channel_data.get("videos_count"),
            channel_data.get("channel_description"),
            channel_data.get("playlist_id")
        ))

        playlist_id = channel_data.get("playlist_id")
    
        if playlist_id:
            # Insert data into the playlist table
            cursor.execute('''
                INSERT INTO playlist (playlist_id, channel_id, channel_name)
                VALUES (?, ?, ?)
            ''', (
                channel_data.get("playlist_id"),
                channel_data.get("channel_Id"),
                channel_data.get("channel_name")
            ))

        playlist_id = channel_data.get("playlist_id")
        if playlist_id:
            # Insert data into the video_details table
            video_data_to_insert = []

            for item in video_details:
                item["ChannelName"] = channel_data.get("channel_name")
                video_data_to_insert.append((
                    item.get("ChannelName"),
                    item.get("Title"),
                    item.get("video_ID"),
                    item.get("video_description"),
                    item.get("Published_date"),
                    item.get("views"),
                    item.get("likes"),
                    item.get("favorite"),
                    item.get("thumbnails"),
                    item.get("duration_seconds"),
                    item.get("comments")
                   
                    
            ))

            cursor.executemany('''
                INSERT INTO video_details (channel_name, Title, video_ID, video_description,
                     Published_date, views, likes, favorite, thumbnails,duration_seconds, comments)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', video_data_to_insert)

          
       
        

        conn.commit()  # Commit changes to the database
        conn.close()  # Close the SQLite connection



# button to switch to the next channel
if st.button("Next Channel"):
    st.session_state.channel_index = (st.session_state.channel_index + 1) % len(channel_ids)
    channel_id = channel_ids[st.session_state.channel_index]
    st.text_input("Enter Channel ID", channel_id)

# Fetch unique channel names from MongoDB collection
channel_names = set(channel.get("channel_name") for channel in collection.find({},
                                         {"channel_name": 1}) if "channel_name" in channel)

# checkbox button for channel selection
selected_channels = st.multiselect("Select Channels", list(channel_names))

# Store selected channels in session state
st.session_state.selected_channels = selected_channels

# button to get SQL data for selected channels
if st.button("Get SQL Data"):
    conn = sqlite3.connect('youtube_db.db')  
    cursor = conn.cursor()

    # Fetch unique channel names from selected channels
    selected_channel_names = st.session_state.selected_channels
    if selected_channel_names:
       
        cursor.execute(f"SELECT DISTINCT * FROM channel_data WHERE channel_name IN ({', '.join(['?']*len(selected_channel_names))})", selected_channel_names)
        channel_data_sql = cursor.fetchall()

        cursor.execute(f"SELECT DISTINCT * FROM playlist WHERE channel_name IN ({', '.join(['?']*len(selected_channel_names))})", selected_channel_names)
        playlist_data_sql = cursor.fetchall()

        cursor.execute(f"SELECT * FROM video_details WHERE channel_name IN ({', '.join(['?']*len(selected_channel_names))})", selected_channel_names)
        video_details_sql = cursor.fetchall()

        

        # Display DataFrames in Streamlit
        st.write("SQLite Channel Data:")
        df_channel = pd.DataFrame(channel_data_sql, columns=["channel_Id", "channel_name", 
                                "subscribers", "views", "videos_count", "channel_description", "playlist_id"])
        st.write(df_channel)

        st.write("SQLite Playlist Data:")
        df_playlist = pd.DataFrame(playlist_data_sql, columns=["playlist_id", "channel_id", "channel_name"])
        st.write(df_playlist)

        
        # Display DataFrames in Streamlit
        st.write("SQLite Video Details:")
        df_video_details = pd.DataFrame(video_details_sql, columns=["channel_name", "Title",
                                             "video_ID", "video_description", "Published_date", "views",
                                             "likes", "favorite", "thumbnails","duration_seconds","comments"])
        st.write(df_video_details)



# Dropdown to select SQL query
sql_query_option = st.selectbox("Select SQL Query", [
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
])

# Function to execute SQL queries and display results as tables
def execute_sql_query(sql_query):
    conn = sqlite3.connect('youtube_db.db')  
    cursor = conn.cursor()

    if sql_query == "1.What are the names of all the videos and their corresponding channels?":
        query = '''
        SELECT v.Title AS Video_Title, v.channel_name AS Channel_Name
        FROM video_details AS v
        '''
        columns = ["Video_Title", "Channel_Name"]
    elif sql_query == "2.Which channels have the most number of videos, and how many videos do they have?":
        query = '''
        SELECT channel_name, COUNT(*) AS Video_Count
        FROM video_details
        GROUP BY channel_name
        ORDER BY Video_Count DESC
        '''
        columns = ["channel_name", "Video_Count"]
    elif sql_query == "3.What are the top 10 most viewed videos and their respective channels?":
        query = '''
        SELECT channel_name AS Channel_Name, Title AS Video_Title, views AS Views
        FROM video_details
        ORDER BY views DESC
        LIMIT 10
        '''
        columns = ["Channel_Name", "Video_Title", "Views"]
    elif sql_query == "4.How many comments were made on each video, and what are their corresponding video names?":
        query = '''
        SELECT channel_name AS Channel_Name, v.Title AS Video_Title, v.comments AS Comment_Count
        FROM video_details AS v
        GROUP BY v.video_ID, v.Title
        '''
        columns = ["channel_name","Video_Title", "Comment_Count"]

    elif sql_query == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query = '''
        SELECT v.Title AS Video_Title, v.channel_name AS Channel_Name, v.likes AS Likes
        FROM video_details AS v
        ORDER BY Likes DESC
        LIMIT 10
        '''
        columns = ["Video_Title", "Channel_Name", "Likes"]
    elif sql_query == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query = '''
        SELECT v.Title AS Video_Title, SUM(v.likes) AS Total_Likes
        FROM video_details AS v
        GROUP BY v.video_ID, v.Title
        '''
        columns = ["Video_Title", "Total_Likes"]
    elif sql_query == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query = '''
        SELECT channel_name, SUM(views) AS Total_Views
        FROM video_details
        GROUP BY channel_name
        '''  
        columns = ["channel_name", "Total_Views"]
    elif sql_query == "8.What are the names of all the channels that have published videos in the year 2022?":
        query = '''
        SELECT DISTINCT channel_name
        FROM video_details
        WHERE strftime('%Y', Published_date) = '2022'
        '''
        columns = ["channel_name"]
    

    elif sql_query == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query = '''
        SELECT v.channel_name AS Channel_Name, AVG(v.duration_seconds) AS Average_Duration
        FROM video_details AS v
        GROUP BY v.channel_name
        ORDER BY Average_Duration DESC
        '''
        columns = ["Channel_Name", "Average_Duration"]

    elif sql_query == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query = '''
        SELECT v.Title AS Video_Title, v.channel_name AS Channel_Name, COUNT(v.comments) AS Comment_Count
        FROM video_details AS v
        GROUP BY v.video_ID, v.Title, v.channel_name
        ORDER BY Comment_Count DESC
        '''
        columns = ["Video_Title", "Channel_Name", "Comment_Count"]



    else:
        st.write("Invalid SQL Query")

    cursor.execute(query)
    query_result = cursor.fetchall()

    if query_result:
        st.write("SQL Query Result:")
        df_query_result = pd.DataFrame(query_result, columns=columns)
        st.write(df_query_result)
    else:
        st.write("No results found for the SQL query.")

    conn.close()  

# Execute SQL query and display result when button is clicked
if st.button("Execute SQL Query"):
    execute_sql_query(sql_query_option)
