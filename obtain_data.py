import requests
import sqlite3 as lite
import time
import datetime
from dateutil.parser import parse

# Connect to database
con = lite.connect('citi_bike.db')
cur = con.cursor()

# Collect data every minute for 60 minutes and add to citi_bike database
for i in range(60):
	# Track request sent
	print "Request {} being made".format(i)

	# Try/Except block to catch timeout errors
	try:
		# Get citibike data
		r = requests.get('http://www.citibikenyc.com/stations/json')

		# Track request received
		print "Parsing request {} at {}".format(i, datetime.datetime.now())

		# Add execution time to table
		exec_time = parse(r.json()['executionTime'])
		with con:
		    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))

		# Loop through stations pulling out ID and available bikes
		for station in r.json()['stationBeanList']:
			station_id = station['id']
			avail_bikes = station['availableBikes']

			# Update table with available bikes for that station
			with con:
				cur.execute("UPDATE available_bikes SET _" + str(station_id) + " = " + str(avail_bikes) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")

		# Track entry added
		print "Entry {} added".format(i)

		# Sleep for 60 seconds minus the ~2.5 seconds that it takes for the program to run
		time.sleep(57.5)

	# Catch and print HTTP timeout errors if the occur
	except requests.exceptions.Timeout:
		print "Request timeout occurred"

