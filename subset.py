""" subset module"""



import pandas as pd
import sqlite3


sqlite_db = 'icesat2_test.sqlite'    # name of the sqlite database file
table_name = 'ATL06'
beam = 'gt1l'

startDate = '2018-10-14 00:20'
endDate = '2018-10-14 00:30'
minLat = 63
maxLat = 66
minLon = -26
maxLon = -12

def connectDB(sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()
    return conn,c


def spaceTime(sqlite_db, table_name, beam,  startDate, endDate, minLat, maxLat, minLon, maxLon):

    conn,c = connectDB(sqlite_db)

    spaceTime_str = "select * FROM " + table_name + " WHERE "
    spaceTime_str += " time < '" + endDate  + "' AND "
    spaceTime_str += " time > '" + startDate  + "' AND "
    spaceTime_str += beam + "_lat < " + str(maxLat)  + " AND "
    spaceTime_str += beam + "_lat > " + str(minLat)  + " AND "
    spaceTime_str += beam + "_lon < " + str(maxLon)  + " AND "
    spaceTime_str += beam + "_lat > " + str(minLon) + " ; "


    df = pd.read_sql_query(spaceTime_str, conn)

    return df

df = spaceTime(sqlite_db, table_name,  beam, startDate, endDate, minLat, maxLat, minLon, maxLon)
