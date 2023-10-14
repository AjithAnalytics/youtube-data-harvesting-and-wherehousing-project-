# Import necessary libraries
import mysql.connector
import pymongo
from googleapiclient.discovery import build
import streamlit as st
import pandas as pd

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)
mycursor = mydb.cursor(buffered=True)

# Connection establishment of mongoDB
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://farmerajith:Sandy@cluster0.xb14cgk.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
except Exception as e:
    st.warning(e)


client=MongoClient("mongodb+srv://farmerajith:Sandy@cluster0.xb14cgk.mongodb.net/?retryWrites=true&w=majority")
db=client.youtube
cal=db.youtube_data

#define api key and Initialize YouTube Data API
api_key = "AIzaSyDuUyRFZ_oxMeNjzsT2PunKp2cxtnQZTV8"
# Define the parameters for the channel request
youtube_api= build('youtube', 'v3', developerKey=api_key)




#get the channel info 
def get_channel_data(youtube_api,channel_id):
    # Make the channel request and get the response
    channel_request=youtube_api.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id)                 
    channel_response=channel_request.execute()
    # extract the channel info
    data=dict(channel_id=channel_id,
                channel_name =channel_response["items"][0]["snippet"]["title"],
                subscription_count = channel_response["items"][0]["statistics"]["subscriberCount"],
                channel_views = channel_response["items"][0]["statistics"]["viewCount"],
                Total_videos=channel_response["items"][0]["statistics"]["videoCount"],
                playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            )
    return data

    # get video ids
def get_video_ids(youtube_api,playlist_id):
    # Make the playlist request and get the response
    playlist_request = youtube_api.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50)                 
    playlist_response=playlist_request.execute()
    video_ids=[]

    #for loop for iterate the response items
    for i in range(len(playlist_response["items"])):
        video_ids.append(playlist_response["items"][i]["snippet"]["resourceId"]["videoId"])

    next_page_token=playlist_response.get('nextPageToken')
    more_pages=True

    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            playlist_request = youtube_api.playlistItems().list(
                                part='snippet',
                                playlistId=playlist_id,
                                maxResults=50,
                                pageToken=next_page_token)                 
            playlist_response=playlist_request.execute()

            for i in range(len(playlist_response["items"])):
                video_ids.append(playlist_response["items"][i]["snippet"]["resourceId"]["videoId"])

            next_page_token=playlist_response.get("nextPageToken")  

    return video_ids


#get video info
def get_video_details(youtube_api,video_ids,max_videos):
    all_video_stats=[]

    #Make the video request and get the response
    for video_id in video_ids[:max_videos]:
        video_request = youtube_api. videos().list(
                        part= "snippet,statistics,contentDetails",
                        id=video_id,
                        maxResults=max_videos)               
        video_response=video_request.execute()

        #Extract the video info
        for video in video_response["items"]:
            video_stats=dict( Video_Id = video_id,
                            Title=video["snippet"]["title"],
                            published_date=video["snippet"]["publishedAt"],
                            view_count = video["statistics"]["viewCount"],
                            like_count = video["statistics"]["likeCount"],
                            favorite_count = video["statistics"]["favoriteCount"],
                            comment_count = video["statistics"]["commentCount"],
                            duration = video["contentDetails"]["duration"],
                            thumbnail = video["snippet"]["thumbnails"]["default"]["url"],
                            caption_status = video["contentDetails"]["caption"]
                            )
            
            all_video_stats.append(video_stats)
        
    return all_video_stats

#get the comment info

def get_video_comments(youtube_api, video_ids,max_videos,max_comment):
    comments = []

    for video_id in video_ids[:max_videos]:
        # Make a request to the commentThreads endpoint to fetch comments
        comments_request = youtube_api.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText',
            maxResults=max_comment 
        )

        # Execute the request and get the response
        comments_response = comments_request.execute()

        # Iterate through the comments and store them
        for comment in comments_response['items']:
            video_ID=video_id
            comment_id=comment['id']
            snippet = comment['snippet']['topLevelComment']['snippet']
            author = snippet['authorDisplayName']
            text = snippet['textDisplay']
            comments.append({'video_ID':video_ID,'comment_id':comment_id,'Author': author, 'Comment': text})

    return comments

#function to fetch channel info video info and comment info 
def overall_channel_data(youtube_api,channel_id):
    #function call and store the data to variable(channel_data)
    channel_Data=(get_channel_data(youtube_api,channel_id))

    #convert data into dataframe
    channel_datafram=pd.DataFrame(channel_Data,index=[1])
    playlist_id=channel_datafram.iloc[0,5]

    #function call and store the data to variable(video_ids)
    video_ids=get_video_ids(youtube_api,playlist_id)

    #function call and store the data to variable(video_data)
    Video_Data=get_video_details(youtube_api,video_ids,max_videos=11)

    #function call and store the data to variable(comment_data)
    comment_Data=get_video_comments(youtube_api, video_ids,max_videos=10,max_comment=5)


    channel_details= {'_id':channel_id,'channel_Data':channel_Data,'Video_Data':Video_Data,'comment_Data':comment_Data}

    return channel_details

#create streamlit webpage
st.title("YOUTUBE DATA HARVESTING AND WHEREHOUSING") 
st.sidebar.subheader("GIVEN DETAILS BELOW")
channel_id=st.sidebar.text_input("ENTER THE CHANNEL ID")
if st.sidebar.button("GET"): 
    try:
        channel_details=overall_channel_data(youtube_api,channel_id)
        st.success("CHANNEL DATA SUCCESSFULLY RETRIVED")
        st.write(channel_details)
    except Exception as e:
        st.warning("Enter valide channel Id")
    #create streamlit options 
option=st.sidebar.selectbox("IF YOU WANT TO ACCESS DATA CHECK OPTION BELOW:",("Choose options",
                                                            "1.Store data to mongoDB",
                                                            "2.Migrating mongoDB data into MYSQL",
                                                            "3.SQL queries"))

if option=="1.Store data to mongoDB":
    select = st.selectbox("IF YOU WANT TO STORE THE DATA", ("PICK YES OR NO", "YES", "NO"))
    if select == "YES":
        #Insert the data into MongoDB
        try:
            channel_details=overall_channel_data(youtube_api,channel_id)
            cal.insert_one(channel_details)
            st.success("Data inserted successfully into MongoDB!")
        except pymongo.errors.DuplicateKeyError:
            st.warning("The document already exists in the MongoDB collection.")
            # Handle the error as needed, or simply continue with your script
            pass  # Do nothing and continue with the script

    if select=="NO":
        st.warning("IF NOT STORE THE DATA DOES NOT MOVE FURTHER PROCCES")

elif option=="2.Migrating mongoDB data into MYSQL":

    #migration proccess
    channel_info=cal.find_one({'_id':channel_id})

    # Extract channeldata from the MongoDB document
    channel_yt_Data= channel_info["channel_Data"]
    #datatype convertion 

    channel_yt_Data['subscription_count']=pd.to_numeric(channel_yt_Data['subscription_count'])
    channel_yt_Data["channel_views"]=pd.to_numeric(channel_yt_Data["channel_views"])
    channel_yt_Data["Total_videos"]=pd.to_numeric(channel_yt_Data["Total_videos"])

    channel_id = channel_yt_Data["channel_id"]
    channel_name = channel_yt_Data["channel_name"]
    subscription_count = channel_yt_Data['subscription_count']
    channel_views = channel_yt_Data["channel_views"]
    Total_videos = channel_yt_Data["Total_videos"]

    #database creation of MYSQL
    mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_data')
 
    mycursor.execute("USE youtube_data")
    mydb.commit()

    #create channel table in database
    column_query="CREATE TABLE IF NOT EXISTS channel (channel_id VARCHAR(50) PRIMARY KEY,channel_name VARCHAR(255), subscription_count INT,channel_views INT,total_videos INT)"
    mycursor.execute(column_query)
    
    #insert data into channel table
    try:
        channel_insert_query = f"INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, Total_videos) VALUES ('{channel_id}', '{channel_name}', {subscription_count}, {channel_views}, {Total_videos})"
        mycursor.execute(channel_insert_query)
        mydb.commit()
    except mysql.connector.Error as err:
        if err.errno== 1062: 
            pass    

    #extract playlist data into mongoDB document
    playlist_id=channel_yt_Data["playlist_id"]

    #playlist table creation in database
    playlist_query="CREATE TABLE IF NOT EXISTS playlist(playlist_id VARCHAR(100) PRIMARY KEY, channel_id VARCHAR(100), FOREIGN KEY (channel_id) REFERENCES channel(channel_id))"
    mycursor.execute(playlist_query)
    
    #Insert data into the playlist table
    try:
        playlist_insert_query = f"INSERT INTO playlist( playlist_id,channel_id) VALUES ('{playlist_id}', '{channel_id}')"
        mycursor.execute(playlist_insert_query)
        mydb.commit()
    except mysql.connector.Error as err:
        if err.errno== 1062: 
            pass    

    #extract video_data from the mongoDB document
    video_yt_data=channel_info['Video_Data']
    #create the video table in database
    video_query="""CREATE TABLE IF NOT EXISTS video (video_Id VARCHAR(50) PRIMARY KEY,
                Title VARCHAR(255),published_date DATETIME ,
                view_count INT,like_count INT,favorite_count INT,
                comment_count INT,duration INT, thumbnail VARCHAR(225),
                caption_status VARCHAR(225),playlist_id VARCHAR(100),
                FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id))"""

    mycursor.execute(video_query)

    #insert video_data into video table in database
    try:
        for row in video_yt_data:
            video_insert_query =f"""INSERT INTO video(Video_Id,Title,published_date,view_count,like_count,favorite_count,
                            comment_count,duration, thumbnail,
                            caption_status,playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'{playlist_id}')"""
            
            values = (row['Video_Id'], row['Title'],pd.to_datetime(row['published_date']),int(row['view_count']),
                    int(row['like_count']),int(row['favorite_count']),int(row['comment_count']),row['duration'],
                    row['thumbnail'],row['caption_status'])
            mycursor.execute(video_insert_query, values)
            mydb.commit()
    except mysql.connector.Error as err:
        if err.errno== 1062: 
            pass    


    comment_yt_Data= channel_info["comment_Data"]

    # Create a table for comments in database
    comment_query = """
            CREATE TABLE IF NOT EXISTS comments(
            comment_id VARCHAR(50) PRIMARY KEY,
            Author VARCHAR(255),
            Comment TEXT,
            video_id VARCHAR(50),
            FOREIGN KEY (video_id) REFERENCES video(video_Id)
        )"""
    mycursor.execute(comment_query)
    try:
    # Insert data into comment table in database
        for row in comment_yt_Data:
            comment_insert_query ="""INSERT INTO comments(Video_Id,comment_id,Author,Comment) VALUES (%s, %s, %s, %s)"""
            values = (row['video_ID'], row['comment_id'], row['Author'], row['Comment'])
            mycursor.execute(comment_insert_query, values)
            mydb.commit()
    except mysql.connector.Error as err:
        if err.errno== 1062: 
            pass 

    st.success("MIGRATION DONE SUCCESSFULLY")

elif option=="3.SQL queries":
    mycursor.execute("USE youtube_data")
    mydb.commit()
    #create streamlit queries options
    questions= st.selectbox("MYSQL QUERIES", 
                            ("CHOOSE THE QUESTIONS","1.What are the names of all the videos and their corresponding channels?",
                             "2.Which channels have the most number of videos, and how many videos dothey have?",
                             "3.What are the top 10 most viewed videos and their respective channels?",
                             "4.How many comments were made on each video, and what are theircorresponding video names?",
                             "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                             "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                             "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                             "8.What are the names of all the channels that have published videos in the year2022",
                             "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                             "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                             ))
    


    if questions == "1.What are the names of all the videos and their corresponding channels?":
        
        #query 1
        mycursor.execute("""SELECT channel.channel_name ,video.Title 
                        FROM channel
                        INNER JOIN playlist ON channel.channel_id = playlist.channel_id
                        INNER JOIN video ON video.playlist_id=playlist.playlist_id
                        GROUP BY video.video_id""")
        out=mycursor.fetchall()
        Q1=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q1)

    elif questions=="2.Which channels have the most number of videos, and how many videos dothey have?":
        #query 2
        mycursor.execute("""SELECT channel_name,Total_videos
                        FROM channel WHERE Total_videos=(select max(Total_videos) from channel)""")
        out=mycursor.fetchall()
        Q2=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q2)

    elif questions== "3.What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("""SELECT channel.channel_name, video.video_id, video.Title, video.view_count
                    FROM channel
                    INNER JOIN playlist ON channel.channel_id = playlist.channel_Id
                    INNER JOIN video ON playlist.playlist_id = video.playlist_Id
                    ORDER BY video.view_count DESC
                    LIMIT 10""")
        out=mycursor.fetchall()
        Q3=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q3)

    elif questions== "4.How many comments were made on each video, and what are theircorresponding video names?":
        mycursor.execute("""SELECT video.video_id, video.Title, COUNT(comments.comment_id) AS Comment_Count
                    FROM video
                    LEFT JOIN comments ON video.video_id = comments.video_ID
                    GROUP BY video.video_id, video.Title""")
        out=mycursor.fetchall()
        Q4=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q4)

    elif questions== "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("""SELECT video.Title, video.like_count, channel.channel_name
                    FROM channel
                    INNER JOIN playlist ON channel.channel_id = playlist.channel_Id
                    INNER JOIN video ON video.playlist_id = playlist.playlist_id
                    WHERE video.like_count = (SELECT MAX(like_count) FROM video)""")
        out=mycursor.fetchall()
        Q5=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q5)


    elif questions== "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT video.Title, SUM(video.like_count) as total_likes
                    FROM video
                    GROUP BY video.Title""")
        out=mycursor.fetchall()
        Q6=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q6)

    elif questions== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel.channel_name,channel.channel_views FROM channel ORDER BY channel_views DESC""")
        out=mycursor.fetchall()
        Q7=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q7)

    elif questions== "8.What are the names of all the channels that have published videos in the year2022":
        mycursor.execute("""SELECT DISTINCT channel.channel_name
                        FROM channel 
                        JOIN playlist  ON channel.channel_id = playlist.channel_id
                        JOIN video  ON playlist.playlist_id = video.playlist_id
                        WHERE YEAR(video.published_date) = 2022;""")
        out=mycursor.fetchall()
        Q8=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q8)  

    
    elif questions== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel.channel_name,video.video_Id,video.Title,AVG(duration) AS average_duration
                    FROM channel
                    INNER JOIN playlist ON channel.channel_id = playlist.channel_Id
                    INNER JOIN video ON video.playlist_id = playlist.playlist_id
                    GROUP BY channel.channel_name""")
        out=mycursor.fetchall()
        Q9=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q9)  
    
    elif questions== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel.channel_name, video.video_Id, video.Title, video.comment_count
                        FROM channel
                        INNER JOIN playlist ON channel.channel_id = playlist.channel_Id
                        INNER JOIN video ON video.playlist_id = playlist.playlist_id
                        WHERE video.comment_count = (SELECT MAX(comment_count) FROM video);
                        """)
        out=mycursor.fetchall()
        Q10=pd.DataFrame(out,columns=[i[0] for i in mycursor.description])
        st.table(Q10)  
