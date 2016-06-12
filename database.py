import requests
import pandas as pd
from pandas.io.json import json_normalize
import sqlite3 as lite

# Get Citibike Data
r = requests.get('http://www.citibikenyc.com/stations/json')
df = json_normalize(r.json()['stationBeanList'])

# Create citibike_reference table and schema
con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
	cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

# SQL statement for inserting rows in citibike_reference
# The ???'s are called a paramterized query, which will match up with values passed in after the sql statement in cur.execute()
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

# Loop through data and add each station to citibike_reference
with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

# Extract IDs from DF and put into list
station_ids = df['id'].tolist()

# Add prefixed '_' to station name and data type - this is going to have us add a separate col for each station id
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

# Create table, adding in a column for each station id with INT
# Join is a method attached to a separator that inserts that separateor in between the sequence of strings passed to it 
with con:
	cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")
