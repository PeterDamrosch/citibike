import pandas as pd
import sqlite3 as lite
import matplotlib.pyplot as plt

con = lite.connect('citi_bike.db')
cur = con.cursor()

# Get citibike usage data from available_bikes table
df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')

# Create a df for the change in usage (absolute value) from each time to the next
# dropping the first row since there's no difference at time 0
df2 = df.copy()[1: ]
for i in range(1,len(df)):
	df2.iloc[i-1] = abs(df.iloc[i] - df.iloc[i-1])

# Create a dictionary with the total activity for each station
total_activity = {}
for col in df2.columns:
	total_activity[col] = df2[col].sum()

# Check out distribution
plt.bar(range(len(total_activity)), total_activity.values())
plt.show()

# Get station with highest activity
most_used = max(total_activity, key=total_activity.get)

# Graph activity level for that station
plt.figure()
plt.bar(range(len(df2)), df2[most_used])
plt.show()

# Get info on the most_used station from the citibike_reference table
with con:
	cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (most_used[1: ],))
data = cur.fetchone()
print "The most active bike station is station id {} at {}, latitude: {}, longitude: {}.".format(data[0],data[1],data[2],data[3])
