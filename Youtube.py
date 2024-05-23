from googleapiclient.discovery import build
import mysql.connector
from mysql.connector import Error
import streamlit as st
import pandas as pd
import time
from datetime import datetime
import isodate

def Api_connect():
    Api_Id="AIzaSyCbOcEf6e8PzWnUTGns3egEAQT64_Nn438"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#Fetching channel information
def get_channel_stats(channel_id):
  all_data = []
  request = youtube.channels().list(
      part="snippet,contentDetails,statistics,status",
      id=channel_id)
  response = request.execute()
  for item in response['items']:
    data = dict(
        channel_name = item['snippet']['title'],
        channel_id = item['id'],
        Subscription_count = item['statistics']['subscriberCount'],
        thumbnail=item['snippet']['thumbnails']['default']['url'],
        channel_views = item['statistics']['viewCount'],
        channel_description = item['snippet']['description'],
        channel_status = item['status']['privacyStatus'],
        total_videos=item['statistics']['videoCount'],
        subscribers=item['statistics']['subscriberCount'],
        views=item['statistics']['viewCount']
    )
    all_data.append(data)
  return all_data

def get_video_ids(channel_id):
  video_ids = []
  request=youtube.channels().list(id=channel_id,part='contentDetails')
  response1 = request.execute()
  Playlist_Id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token = None

  while True:
    request = youtube.playlistItems().list(part = 'snippet',playlistId = Playlist_Id, pageToken = next_page_token)
    response2 = request.execute()
    for item in response2['items']:
      video_ids.append((item['snippet']['resourceId']['videoId']))
    next_page_token = response2.get('nextPageToken')

    if next_page_token is None:
      break

  return video_ids

def get_video_info(video_ids):
    video_data = []
    for i in range(0, len(video_ids),50):
        video_id_batch = video_ids[i:i + 50]
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics,status",
            id=','.join(video_id_batch)).execute()
        for video in response['items']:
            video_id = video['id']
            playlist_id = None

            published_date = video['snippet'].get('publishedAt')
            if published_date:
                published_date = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")

            duration = video['contentDetails']['duration']
            parsed_duration = isodate.parse_duration(duration)

            video_details = dict(
                channel_name=video['snippet']['channelTitle'],
                channel_id=video['snippet']['channelId'],
                playlist_id=playlist_id,
                video_id=video['id'],
                title=video['snippet']['title'],
                tags=video['snippet'].get('tags'),
                thumbnail=video['snippet']['thumbnails']['default']['url'],
                description=video['snippet']['description'],
                published_date=published_date,
                duration=int(parsed_duration.total_seconds()),
                views=video['statistics']['viewCount'],
                likes=video['statistics'].get('likeCount'),
                dislikes = video['statistics'].get('dislikeCount'),
                comments=video['statistics'].get('commentCount'),
                favorite_count=video['statistics']['favoriteCount'],
                caption_status=video['contentDetails']['caption']
            )
            video_data.append(video_details)
    return video_data


def get_comment_info(video_ids):
    comment_data = []
    for video_id in video_ids:
        next_page_token = None
        while True:
            try:
                request = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()
                for item in response['items']:
                    comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt']
                    if comment_published:
                        comment_published = datetime.strptime(comment_published, "%Y-%m-%dT%H:%M:%SZ")

                    data = dict(
                        comment_id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published=comment_published,
                        reply_count=item['snippet']['totalReplyCount']
                    )
                    comment_data.append(data)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token or len(comment_data) >= 100:
                    break
            except Exception as e:
                print(f"An error occurred: {e}")
                break
    return comment_data

#function to create db connection
def create_db_connection(host_name,port_name,user_name,user_password,database_name):
  connection = None
  try:
    connection = mysql.connector.connect(
        host = 'localhost',
        port = 3306,
        user = 'Mdms',
        password = 'Mdms@05072021',
        database ='guvi'
    )

    alert = st.info("Connection to MySQL DB successfull")
    time.sleep(1)
    alert.empty()

  except Error as e:
    alert = st.error(f'Error connecting to MySQL: {e}')
    time.sleep(1)
    alert.empty()

  return connection

#Table creation in MySQL
def create_tables(connection):
  create_channel_table = """
  CREATE TABLE IF NOT EXISTS Channel_table (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    channel_description TEXT,
    channel_views BIGINT,
    channel_subscriber_count INT,
    channel_status VARCHAR(255)
  )
  """
  create_comment_table = """
  CREATE TABLE IF NOT EXISTS Comment_table (
    comment_id VARCHAR(255) PRIMARY KEY,
    video_id VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_date DATETIME
  )
  """
  create_video_table = """
  CREATE TABLE IF NOT EXISTS Video_table (
    video_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    playlist_id VARCHAR(255),
    video_name VARCHAR(255),
    video_description VARCHAR(10000),
    published_date DATETIME,
    view_count INT,
    like_count INT,
    dislike_count INT,
    favorite_count INT,
    comment_count INT,
    duration INT,
    thumbnail VARCHAR(255),
    caption_status VARCHAR(255)
  )
  """
  execute_query(connection, create_channel_table)
  execute_query(connection, create_comment_table)
  execute_query(connection, create_video_table)

#function to execute the query
def execute_query(connection, query, data = None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()
        query_alert = st.info("Query executed successfully")
        time.sleep(1)
        query_alert.empty()
    except Error as e:
        error_alert = st.error(f"Error executing query: {e}")
        time.sleep(1)
        error_alert.empty()

def insert_channel_data(connection, channel_data) :
  query = """
  INSERT INTO Channel_table(
    channel_id, channel_name, channel_description,channel_subscriber_count,
    channel_views, channel_status
  )
  VALUES (%s, %s, %s, %s, %s, %s)
  """
  channel_info = (
      channel_data[0]["channel_id"],
      channel_data[0]['channel_name'],
      channel_data[0]['channel_description'],
      channel_data[0]['subscribers'],
      channel_data[0]['views'],
      channel_data[0]['channel_status']
  )

  execute_query(connection,query,channel_info)
  connection.commit()
  alert = st.success("Data inserted into Channel table successfully")
  time.sleep(1)
  alert.empty()

def insert_comment_data(connection, comment_data):
  cursor = connection.cursor()
  query = """
  INSERT INTO Comment_table(
     comment_id, video_id, comment_text, comment_author, comment_published_date
  )
  VALUES(%s,%s,%s,%s,%s)
  """
  for item in comment_data:
    comment_info = (
        item['comment_id'],
        item['video_id'],
        item['comment_text'],
        item['comment_author'],
        item['comment_published']
    )
    cursor.execute(query, comment_info)
  connection.commit()
  alert = st.success("Data inserted into Comment table successfully")
  time.sleep(1)
  alert.empty()

def insert_video_data(connection,video_data):
  cursor = connection.cursor()
  query = """
  INSERT INTO VIDEO_TABLE(
    video_id ,channel_name,playlist_id,video_name,video_description,
    published_date,view_count,like_count ,dislike_count,
    favorite_count,comment_count,duration ,thumbnail,
    caption_status
  )
  VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
  """
  for item in video_data:
    video_info = (
        item['video_id'],
        item['channel_name'],
        item['playlist_id'],
        item['title'],
        item['description'],
        item['published_date'].strftime('%Y-%m-%d %H:%M:%S'),
        item['views'],
        item['likes'],
        item['dislikes'],
        item['favorite_count'],
        item['comments'],
        item['duration'],
        item['thumbnail'],
        item['caption_status']
    )
    cursor.execute(query,video_info)
  connection.commit()
  alert = st.success("Data inserted into Video table successfully")
  time.sleep(1)
  alert.empty()

def main():
      page = st.sidebar.radio("Navigation", ["Home", "Youtube Data Harvesting and Loading into MySQL", "Questions"])

      if page == 'Home' :
        st.title("YouTube Data Analysis")
        st.subheader("About")
        st.write("This is a Streamlit application for Analyzing YouTube data by")
        st.write(":red[***Sajeetha Begum N***]")
        st.subheader("Project Details")
        details = '''YouTube Data Harvesting and Warehousing using SQL and Streamlit'''
        st.markdown(details)

      elif page == "Youtube Data Harvesting and Loading into MySQL":
        st.title("Fetch Data from YouTube")
        channel_id = st.text_input('Enter the channel_id')

        if st.button('Fetch Data'):
          st.spinner('Fetching data from YouTube API...')

          channel_data = get_channel_stats(channel_id)
          if channel_data:
            st.write("Channel Name:", channel_data[0]['channel_name'])
            st.image(channel_data[0]['thumbnail'])
            st.write("Channel Description:", channel_data[0]['channel_description'])
            st.write("Video Count:", channel_data[0]['total_videos'])
            st.write("Subscriber Count:", channel_data[0]['subscribers'])
            st.write("Channel Views:", channel_data[0]['views'])

        if st.button('Load Data into MySQL'):
          time.sleep(1)
          connection = create_db_connection("localhost",3306, "Mdms", "Mdms@05072021", "guvi")
          if connection:
            create_tables(connection)

            starting_alert = st.info("Inserting data into MySQL")
            time.sleep(1)
            starting_alert.empty()

            channel_data = get_channel_stats(channel_id)
            insert_channel_data(connection, channel_data)

            video_ids = get_video_ids(channel_id)
            video_data = get_video_info(video_ids)
            insert_video_data(connection, video_data)

            comment_data = get_comment_info(video_ids)
            insert_comment_data(connection, comment_data)
            connection.close()

            Final_completion_alert = st.success("Data insertion process completed!")
            time.sleep(1)
            Final_completion_alert.empty()

      elif page == "Questions":
        st.title("Questions")
        connection = create_db_connection("localhost",3306, "Mdms", "Mdms@05072021", "guvi")
        time.sleep(1)
        questions = [
            'Select a Question',
            '1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
               ]
        selected_question = st.selectbox("Select a question:", questions)
        if selected_question:
            cursor = connection.cursor()
            if selected_question == '1. What are the names of all the videos and their corresponding channels?':
                cursor.execute("""
                SELECT video_name AS Video_Title, c.channel_name AS Channel_Name FROM video_table v
                inner join channel_table c on v.channel_name = c.channel_name ORDER BY c.channel_name, v.video_name
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '2. Which channels have the most number of videos, and how many videos do they have?':
                cursor.execute("""
                SELECT Channel_Name, Num_of_Videos FROM (
                SELECT Channel_Name, Num_of_Videos, ROW_NUMBER() OVER (ORDER BY Num_of_Videos DESC) AS RANK_NUMBER
                FROM (
                SELECT channel_name AS Channel_Name, COUNT(video_id) AS Num_of_Videos
                FROM video_table group by 1
                ) AS ranked) AS FINAL
                WHERE RANK_NUMBER = 1;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '3. What are the top 10 most viewed videos and their respective channels?':
                cursor.execute("""
                SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count as views
                    FROM video_table ORDER BY views DESC LIMIT 10;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '4. How many comments were made on each video, and what are their corresponding video names?':
                cursor.execute("""
                SELECT a.video_name AS Video_Title, b.Total_Comments
                    FROM video_table AS a LEFT JOIN (SELECT video_id, COUNT(comment_id) AS Total_Comments
                    FROM comment_table GROUP BY video_id) AS b ON a.video_id = b.video_id
                    WHERE b.Total_Comments IS NOT NULL ORDER BY b.Total_Comments DESC;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                cursor.execute("""
                SELECT channel_name AS Channel_Name, video_name AS Title, Like_Count
                FROM video_table ORDER BY Like_Count DESC LIMIT 10;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '6. What is the total number of likes for each video, and what are their corresponding video names?':
                cursor.execute("""
                SELECT video_name, Like_Count FROM video_table
                where Like_Count is not null ORDER BY Like_Count DESC;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                cursor.execute("""
                SELECT channel_name AS Channel_Name, channel_views as Views FROM channel_table;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '8. What are the names of all the channels that have published videos in the year 2022?':
                cursor.execute("""
                SELECT channel_name AS Channel_Name FROM video_table WHERE published_date LIKE '2022%'
                GROUP BY channel_name ORDER BY channel_name;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                cursor.execute("""
                SELECT channel_name, SUM(duration) / COUNT(*) AS average_duration
                FROM video_table GROUP BY channel_name;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            elif selected_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                cursor.execute("""
                SELECT channel_name AS Channel_Name, v.video_id AS Video_ID, count(c.comment_id) AS Num_of_Comments
                FROM video_table v inner join comment_table c on v.video_id = c.video_id
                group by 1, 2 ORDER BY Num_of_Comments DESC LIMIT 10;
                """)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                st.write(df)
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()