""" subset module"""

""" once user has created db, tihs is one of the core modules to subset/explore data"""
""" user input icesat2py.subset(which table, which vars (beam 1-6), startTime, endTime, startLat,startLon, endLat,endLon, opt:export?)
returns df?

https://n5eil01u.ecs.nsidc.org/ATLAS/ATL03.001/


"""

import pandas as pd
import sqlite3


sqlite_db = 'icesat2_test.sqlite'    # name of the sqlite database file
table_name = 'ATL06'




def connectDB(sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()
    return conn,c


# def spaceTime(sqlite_db, table_name,  startDate, endDate, lat1, lat2, lon1, lon2):
def spaceTime(sqlite_db, table_name):


    conn,c = connectDB(sqlite_db)
    df = pd.read_sql_query('select * from ' + table_name + ' ;', conn)

    return df

df = spaceTime(sqlite_db, table_name)
