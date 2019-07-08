""" build sqlite db module"""

"""user input? None? checks in ./data folder for .h5 files. checks if sqlite db named <?> exists
if files exist and db does not exist: create sqllite db with name <?>
for every (hardcoded?) data product. ie laser returns, gridded data, etc... write sqlite table

- for every list h5files:
    import with h5py, write lat/lon/elev/var1/var2/var3/var... to specific data product table
    -create index on space/time for faster subset




###########################

To do next time


It looks like the epoch_offset was successful. The astropy date converter to UTC isn't working now. Also do the beams have diff times? It would be nice to have a single time... Probably wouldn't make a huge diff.

After DT is fixed, update subset.py to have args for Space/time"""

###########################



import os
import pandas as pd
import h5py
import sqlite3
import glob
from astropy.time import Time

inputdir = os.getcwd() + '/Outputs/'
hdf5_file_list = glob.glob(inputdir + '*.h5*')

def read_hdf5_headers():
    f = h5py.File(hdf5_file_list[0], 'r')
    l1_keys = f.keys()
    return l1_keys



l1_keys = read_hdf5_headers()

sqlite_db = 'icesat2_test.sqlite'    # name of the sqlite database file
table_name = 'ATL06'

def connectDB(sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()
    return conn,c


def build_sqliteDB(sqlite_db, table_name):
    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()

    sqlite_str = """CREATE TABLE IF NOT EXISTS {tn} (
    {time} {time_ft},
    {gt1l_lat} {gt1l_lat_ft},
    {gt1l_lon} {gt1l_lon_ft},
    {gt1l_h_li} {gt1l_h_li_ft},
    {gt1l_x_atc} {gt1l_x_atc_ft},
    {gt1l_atl06_qual} {gt1l_atl06_qual_ft},
    {gt1r_lat} {gt1r_lat_ft},
    {gt1r_lon} {gt1r_lon_ft},
    {gt1r_h_li} {gt1r_h_li_ft},
    {gt1r_x_atc} {gt1r_x_atc_ft},
    {gt1r_atl06_qual} {gt1r_atl06_qual_ft},
    {gt2l_lat} {gt2l_lat_ft},
    {gt2l_lon} {gt2l_lon_ft},
    {gt2l_h_li} {gt2l_h_li_ft},
    {gt2l_x_atc} {gt2l_x_atc_ft},
    {gt2l_atl06_qual} {gt2l_atl06_qual_ft},
    {gt2r_lat} {gt2r_lat_ft},
    {gt2r_lon} {gt2r_lon_ft},
    {gt2r_h_li} {gt2r_h_li_ft},
    {gt2r_x_atc} {gt2r_x_atc_ft},
    {gt2r_atl06_qual} {gt2r_atl06_qual_ft},
    {gt3l_lat} {gt3l_lat_ft},
    {gt3l_lon} {gt3l_lon_ft},
    {gt3l_h_li} {gt3l_h_li_ft},
    {gt3l_x_atc} {gt3l_x_atc_ft},
    {gt3l_atl06_qual} {gt3l_atl06_qual_ft},
    {gt3r_lat} {gt3r_lat_ft},
    {gt3r_lon} {gt3r_lon_ft},
    {gt3r_h_li} {gt3r_h_li_ft},
    {gt3r_x_atc} {gt3r_x_atc_ft},
    {gt3r_atl06_qual} {gt3r_atl06_qual_ft}  );""".format(tn=table_name,
    time='time', time_ft='TEXT',
    gt1l_lat='gt1l_lat', gt1l_lat_ft='REAL',
    gt1l_lon='gt1l_lon', gt1l_lon_ft='REAL',
    gt1l_h_li='gt1l_h_li', gt1l_h_li_ft='REAL',
    gt1l_x_atc='gt1l_x_atc', gt1l_x_atc_ft='REAL',
    gt1l_atl06_qual='gt1l_atl06_quality_summary', gt1l_atl06_qual_ft='INT',
    gt1r_lat='gt1r_lat', gt1r_lat_ft='REAL',
    gt1r_lon='gt1r_lon', gt1r_lon_ft='REAL',
    gt1r_h_li='gt1r_h_li', gt1r_h_li_ft='REAL',
    gt1r_x_atc='gt1r_x_atc', gt1r_x_atc_ft='REAL',
    gt1r_atl06_qual='gt1r_atl06_quality_summary', gt1r_atl06_qual_ft='INT',
    gt2l_lat='gt2l_lat', gt2l_lat_ft='REAL',
    gt2l_lon='gt2l_lon', gt2l_lon_ft='REAL',
    gt2l_h_li='gt2l_h_li', gt2l_h_li_ft='REAL',
    gt2l_x_atc='gt2l_x_atc', gt2l_x_atc_ft='REAL',
    gt2l_atl06_qual='gt2l_atl06_quality_summary', gt2l_atl06_qual_ft='INT',
    gt2r_lat='gt2r_lat', gt2r_lat_ft='REAL',
    gt2r_lon='gt2r_lon', gt2r_lon_ft='REAL',
    gt2r_h_li='gt2r_h_li', gt2r_h_li_ft='REAL',
    gt2r_x_atc='gt2r_x_atc', gt2r_x_atc_ft='REAL',
    gt2r_atl06_qual='gt2r_atl06_quality_summary', gt2r_atl06_qual_ft='INT',
    gt3l_lat='gt3l_lat', gt3l_lat_ft='REAL',
    gt3l_lon='gt3l_lon', gt3l_lon_ft='REAL',
    gt3l_h_li='gt3l_h_li', gt3l_h_li_ft='REAL',
    gt3l_x_atc='gt3l_x_atc', gt3l_x_atc_ft='REAL',
    gt3l_atl06_qual='gt3l_atl06_quality_summary', gt3l_atl06_qual_ft='INT',
    gt3r_lat='gt3r_lat', gt3r_lat_ft='REAL',
    gt3r_lon='gt3r_lon', gt3r_lon_ft='REAL',
    gt3r_h_li='gt3r_h_li', gt3r_h_li_ft='REAL',
    gt3r_x_atc='gt3r_x_atc', gt3r_x_atc_ft='REAL',
    gt3r_atl06_qual='gt3r_atl06_quality_summary', gt3r_atl06_qual_ft='INT'
    )

    c.execute(sqlite_str)


    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()




build_sqliteDB(sqlite_db, table_name)




"""
atlas_sdp_gps_epoch

Number of GPS seconds between the GPS epoch (1980­01­
06T00:00:00.000000Z UTC) and the ATLAS Standard Data
Operations Product (SDP) epoch (2018­01­01:T00.00.00.000000 UTC).
Add this value to delta time parameters to compute full
gps_seconds (relative to the GPS epoch) for each data point

"""


def insert_into_sqlite(table_name, df,conn):
    df.to_sql(table_name,conn, if_exists='append', index=False)

def convert_gps_to_datetime(col):
    col = Time(Time(col,format='gps'),format='iso')
    return col

def build_dataframe_atl06(sqlite_db, table_name):
    for file in hdf5_file_list:
        try:
            conn = sqlite3.connect(sqlite_db)
            print('Inserting ' + file +  ' into db')

            c = conn.cursor()
            f = h5py.File(file, 'r')
            columns = ['time','gt1l_lat','gt1l_lon','gt1l_h_li','gt1l_x_atc', 'gt1l_atl06_quality_summary',
            'gt1r_lat','gt1r_lon','gt1r_h_li','gt1r_x_atc', 'gt1r_atl06_quality_summary',
            'gt2l_lat','gt2l_lon','gt2l_h_li','gt2l_x_atc', 'gt2l_atl06_quality_summary',
            'gt2r_lat','gt2r_lon','gt2r_h_li','gt2r_x_atc', 'gt2r_atl06_quality_summary',
            'gt3l_lat','gt3l_lon','gt3l_h_li','gt3l_x_atc', 'gt3l_atl06_quality_summary',
            'gt3r_lat','gt3r_lon','gt3r_h_li','gt3r_x_atc', 'gt3r_atl06_quality_summary']
            df = pd.DataFrame(columns = columns)
            #gtl1

            """

            df['gtl1_delta_time'] = Time(Time(df['gtl1_delta_time'],format='gps'),format='iso')


            """

            epoch_offset = f['ancillary_data']['atlas_sdp_gps_epoch'][0]


            df['time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['time'] =  Time(Time(df['time'],format='gps'),format='iso')
            df['time'] = df['time'].astype(str)
            df['gt1l_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gt1l_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gt1l_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gt1l_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gt1l_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtr1
            df['gt1r_lat'] = f['gt1r']['land_ice_segments']['latitude']
            df['gt1r_lon'] = f['gt1r']['land_ice_segments']['longitude']
            df['gt1r_h_li'] = f['gt1r']['land_ice_segments']['h_li']
            df['gt1r_x_atc'] = f['gt1r']['land_ice_segments']['ground_track']['x_atc']
            df['gt1r_atl06_quality_summary'] = f['gt1r']['land_ice_segments']['atl06_quality_summary']

            # df['gt2l_lat'] = f['gt2l']['land_ice_segments']['latitude']
            # df['gt2l_lon'] = f['gt2l']['land_ice_segments']['longitude']
            # df['gt2l_h_li'] = f['gt2l']['land_ice_segments']['h_li']
            # df['gt2l_x_atc'] = f['gt2l']['land_ice_segments']['ground_track']['x_atc']
            # df['gt2l_atl06_quality_summary'] = f['gt2l']['land_ice_segments']['atl06_quality_summary']
            #
            # #gtr2
            # df['gt2r_lat'] = f['gt2r']['land_ice_segments']['latitude']
            # df['gt2r_lon'] = f['gt2r']['land_ice_segments']['longitude']
            # df['gt2r_h_li'] = f['gt2r']['land_ice_segments']['h_li']
            # df['gt2r_x_atc'] = f['gt2r']['land_ice_segments']['ground_track']['x_atc']
            # df['gt2r_atl06_quality_summary'] = f['gt2r']['land_ice_segments']['atl06_quality_summary']
            #
            # df['gt3l_lat'] = f['gt3l']['land_ice_segments']['latitude']
            # df['gt3l_lon'] = f['gt3l']['land_ice_segments']['longitude']
            # df['gt3l_h_li'] = f['gt3l']['land_ice_segments']['h_li']
            # df['gt3l_x_atc'] = f['gt3l']['land_ice_segments']['ground_track']['x_atc']
            # df['gt3l_atl06_quality_summary'] = f['gt3l']['land_ice_segments']['atl06_quality_summary']
            # #gtr3
            # df['gt3r_lat'] = f['gt3r']['land_ice_segments']['latitude']
            # df['gt3r_lon'] = f['gt3r']['land_ice_segments']['longitude']
            # df['gt3r_h_li'] = f['gt3r']['land_ice_segments']['h_li']
            # df['gt3r_x_atc'] = f['gt3r']['land_ice_segments']['ground_track']['x_atc']
            # df['gt3r_atl06_quality_summary'] = f['gt3r']['land_ice_segments']['atl06_quality_summary']

            # print(list(df))

            insert_into_sqlite(table_name, df, conn)
            conn.commit()
            conn.close()
        except Exception as e:
            print(e)

        # break


build_dataframe_atl06(sqlite_db, table_name)







def index_sqlite(sqlite_db, table_name):
    conn,c = connectDB(sqlite_db)

    sqlite_str = """
    CREATE  INDEX idx_time
    ON """ + table_name + """ (time);
    """
    c.execute(sqlite_str)
    conn.commit()
    conn.close()
    #eventually build time/space/elevation index # Preformace speedup
    print('Index built on table ' + table_name + ' named idx_time')


index_sqlite(sqlite_db, table_name)
