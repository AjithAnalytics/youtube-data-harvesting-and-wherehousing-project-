
# YOUTUBE DATA HARVESTING AND WHEREHOUSING

## Overview
    This is my intermediate level Python Project to harvest YouTube data using YouTube Data API and store the data in a MongoDB database as a data lake.
    After that the data is migrated from the data lake to a SQL database as tables and are displayed in the streamlit application
## Introduction
    YouTube is the world's most popular video-sharing platform, with over 2 billion active users. 
    It is a valuable source of data for businesses, researchers, and individuals. 
    This project will demonstrate how to harvest and warehouse YouTube data using SQL, MongoDB, and 
## Installation

Install need project packages

```bash
•!pip install mysql.connector-python
•!pip install pymongo
•!pip install googleapiclient.discovery import build
•!pip install streamlit as st
•!pip install pandas as pd 

```
    
## API Reference
	The API provides access to a wide range of data. 
```http
-channel information
-video statistics
-viewer engagement metrics.
```


## Data storage
    The collected data can be stored in a variety of ways but In this project, we will use  
```http
•	MongoDB- MongoDB is a NoSQL database that is well-suited for storing large amounts of unstructured data.
•	SQL-. SQL is a relational database that is well-suited for querying and analyzing structured data.

```
## Database connections
```http
•   You can use the mysql-connector-python library to connect to a MySQL database in Python. If you haven't already installed it, you can do so using 
pip install mysql-connector-python
•   You can use the pymongo library to connect to a MongoDB database in Python. If you haven't already installed it, you can do so using 
pip install pymongo
```
## Programming hints
```http
Write the program to below content
• Retrieve the data from YOUTUBE using API key
• Extract the data and store the data to mangoDB
• Take the data from mangoDB and migration data to MYSQL
• Analyze the SQL quires 
```

## Streamlit
```http
 The data can be analyzed using a variety of tools. In this project, we will use Streamlit. 
 Streamlit is a Python library that can be used to create interactive web applications.  
 We will use Streamlit to create a dashboard that allows users to visualize and analyze the data.
```
## Used By
```http
 This approach can be used to collect large amounts of data from YouTube. 
 The data can be stored in a MongoDB and SQL. 

- The data can be analyzed 
- identify trends
- make predictions
- decision-making.
```
```http
THANK YOU!!!
Hope!! this is useful for you
```