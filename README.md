# YouTube Channel Data Analysis

This project is a Python-based web application for fetching and analyzing data from YouTube channels using the YouTube Data API. It allows you to retrieve channel statistics, video details, and perform SQL queries on the collected data.
# Introduction

YouTube Channel Data Analysis is a Streamlit application that leverages the YouTube Data API to fetch data from specified YouTube channels. The fetched data is then stored in a MongoDB database and SQLite database for further analysis.
# Features

    * Retrieve channel statistics such as subscribers, views, and video count.
    * Fetch video details including title, description, views, likes, and more.
    * Store collected data in MongoDB and SQLite databases.
    * Execute SQL queries on the SQLite database to analyze the data.
    * Visualize data and query results in a user-friendly Streamlit web interface.

# Prerequisites

Before using this project, ensure you have the following dependencies installed:

    Python (3.7+)
    Streamlit
    Google API Client Library
    Pandas
    Pymongo
    SQLite

Additionally, 

you will need to obtain a YouTube Data API key from the Google Developer Console and configure MongoDB connection settings.

To get YouTube channel ID:Simply Go to the channel of that person-> Right-click on any space there-> Click on view page source-> source page will open->press ctrl+f and search ChannelID and press Enter->  The search results will highlight the line in the source code where the Channel ID is mentioned-> Copy the Channel ID from the source code and use it as needed.
