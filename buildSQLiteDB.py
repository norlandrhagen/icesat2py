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




import pandas as pd
import h5py
import sqlite3
import glob
from astropy.time import Time

inputdir = '/home/nrhagen/Documents/icesat2py/Outputs/'
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
    {gtl1_dt} {gtl1_dt_ft},
    {gtl1_lat} {gtl1_lat_ft},
    {gtl1_lon} {gtl1_lon_ft},
    {gtl1_h_li} {gtl1_h_li_ft},
    {gtl1_x_atc} {gtl1_x_atc_ft},
    {gtl1_atl06_qual} {gtl1_atl06_qual_ft},
    {gtr1_dt} {gtr1_dt_ft},
    {gtr1_lat} {gtr1_lat_ft},
    {gtr1_lon} {gtr1_lon_ft},
    {gtr1_h_li} {gtr1_h_li_ft},
    {gtr1_x_atc} {gtr1_x_atc_ft},
    {gtr1_atl06_qual} {gtr1_atl06_qual_ft},
    {gtl2_dt} {gtl2_dt_ft},
    {gtl2_lat} {gtl2_lat_ft},
    {gtl2_lon} {gtl2_lon_ft},
    {gtl2_h_li} {gtl2_h_li_ft},
    {gtl2_x_atc} {gtl2_x_atc_ft},
    {gtl2_atl06_qual} {gtl2_atl06_qual_ft},
    {gtr2_dt} {gtr2_dt_ft},
    {gtr2_lat} {gtr2_lat_ft},
    {gtr2_lon} {gtr2_lon_ft},
    {gtr2_h_li} {gtr2_h_li_ft},
    {gtr2_x_atc} {gtr2_x_atc_ft},
    {gtr2_atl06_qual} {gtr2_atl06_qual_ft},
    {gtl3_dt} {gtl3_dt_ft},
    {gtl3_lat} {gtl3_lat_ft},
    {gtl3_lon} {gtl3_lon_ft},
    {gtl3_h_li} {gtl3_h_li_ft},
    {gtl3_x_atc} {gtl3_x_atc_ft},
    {gtl3_atl06_qual} {gtl3_atl06_qual_ft},
    {gtr3_dt} {gtr3_dt_ft},
    {gtr3_lat} {gtr3_lat_ft},
    {gtr3_lon} {gtr3_lon_ft},
    {gtr3_h_li} {gtr3_h_li_ft},
    {gtr3_x_atc} {gtr3_x_atc_ft},
    {gtr3_atl06_qual} {gtr3_atl06_qual_ft} );""".format(tn=table_name,
    gtl1_dt='gtl1_delta_time', gtl1_dt_ft='REAL',
    gtl1_lat='gtl1_lat', gtl1_lat_ft='REAL',
    gtl1_lon='gtl1_lon', gtl1_lon_ft='REAL',
    gtl1_h_li='gtl1_h_li', gtl1_h_li_ft='REAL',
    gtl1_x_atc='gtl1_x_atc', gtl1_x_atc_ft='REAL',
    gtl1_atl06_qual='gtl1_atl06_quality_summary', gtl1_atl06_qual_ft='INT',
    gtr1_dt='gtr1_delta_time', gtr1_dt_ft='REAL',
    gtr1_lat='gtr1_lat', gtr1_lat_ft='REAL',
    gtr1_lon='gtr1_lon', gtr1_lon_ft='REAL',
    gtr1_h_li='gtr1_h_li', gtr1_h_li_ft='REAL',
    gtr1_x_atc='gtr1_x_atc', gtr1_x_atc_ft='REAL',
    gtr1_atl06_qual='gtr1_atl06_quality_summary', gtr1_atl06_qual_ft='INT',
    gtl2_dt='gtl2_delta_time', gtl2_dt_ft='REAL',
    gtl2_lat='gtl2_lat', gtl2_lat_ft='REAL',
    gtl2_lon='gtl2_lon', gtl2_lon_ft='REAL',
    gtl2_h_li='gtl2_h_li', gtl2_h_li_ft='REAL',
    gtl2_x_atc='gtl2_x_atc', gtl2_x_atc_ft='REAL',
    gtl2_atl06_qual='gtl2_atl06_quality_summary', gtl2_atl06_qual_ft='INT',
    gtr2_dt='gtr2_delta_time', gtr2_dt_ft='REAL',
    gtr2_lat='gtr2_lat', gtr2_lat_ft='REAL',
    gtr2_lon='gtr2_lon', gtr2_lon_ft='REAL',
    gtr2_h_li='gtr2_h_li', gtr2_h_li_ft='REAL',
    gtr2_x_atc='gtr2_x_atc', gtr2_x_atc_ft='REAL',
    gtr2_atl06_qual='gtr2_atl06_quality_summary', gtr2_atl06_qual_ft='INT',
    gtl3_dt='gtl3_delta_time', gtl3_dt_ft='REAL',
    gtl3_lat='gtl3_lat', gtl3_lat_ft='REAL',
    gtl3_lon='gtl3_lon', gtl3_lon_ft='REAL',
    gtl3_h_li='gtl3_h_li', gtl3_h_li_ft='REAL',
    gtl3_x_atc='gtl3_x_atc', gtl3_x_atc_ft='REAL',
    gtl3_atl06_qual='gtl3_atl06_quality_summary', gtl3_atl06_qual_ft='INT',
    gtr3_dt='gtr3_delta_time', gtr3_dt_ft='REAL',
    gtr3_lat='gtr3_lat', gtr3_lat_ft='REAL',
    gtr3_lon='gtr3_lon', gtr3_lon_ft='REAL',
    gtr3_h_li='gtr3_h_li', gtr3_h_li_ft='REAL',
    gtr3_x_atc='gtr3_x_atc', gtr3_x_atc_ft='REAL',
    gtr3_atl06_qual='gtr3_atl06_quality_summary', gtr3_atl06_qual_ft='INT'
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
            columns = ['gtl1_delta_time','gtl1_lat','gtl1_lon','gtl1_h_li','gtl1_x_atc', 'gtl1_atl06_quality_summary',
            'gtr1_delta_time','gtr1_lat','gtr1_lon','gtr1_h_li','gtr1_x_atc', 'gtr1_atl06_quality_summary',
            'gtl2_delta_time','gtl2_lat','gtl2_lon','gtl2_h_li','gtl2_x_atc', 'gtl2_atl06_quality_summary',
            'gtr2_delta_time','gtr2_lat','gtr2_lon','gtr2_h_li','gtr2_x_atc', 'gtr2_atl06_quality_summary',
            'gtl3_delta_time','gtl3_lat','gtl3_lon','gtl3_h_li','gtl3_x_atc', 'gtl3_atl06_quality_summary',
            'gtr3_delta_time','gtr3_lat','gtr3_lon','gtr3_h_li','gtr3_x_atc', 'gtr3_atl06_quality_summary']
            df = pd.DataFrame(columns = columns)
            #gtl1

            """

            df['gtl1_delta_time'] = Time(Time(df['gtl1_delta_time'],format='gps'),format='iso')


            """

            epoch_offset = f['ancillary_data']['atlas_sdp_gps_epoch'][0]

            df['gtl1_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtl1_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtl1_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtl1_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtl1_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtl1_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtr1
            df['gtr1_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtr1_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtr1_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtr1_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtr1_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtr1_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtl2
            df['gtl2_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtl2_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtl2_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtl2_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtl2_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtl2_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtr2
            df['gtr2_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtr2_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtr2_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtr2_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtr2_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtr2_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtl3
            df['gtl3_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtl3_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtl3_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtl3_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtl3_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtl3_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']
            #gtr3
            df['gtr3_delta_time'] = f['gt1l']['land_ice_segments']['delta_time'] + epoch_offset
            df['gtr3_lat'] = f['gt1l']['land_ice_segments']['latitude']
            df['gtr3_lon'] = f['gt1l']['land_ice_segments']['longitude']
            df['gtr3_h_li'] = f['gt1l']['land_ice_segments']['h_li']
            df['gtr3_x_atc'] = f['gt1l']['land_ice_segments']['ground_track']['x_atc']
            df['gtr3_atl06_quality_summary'] = f['gt1l']['land_ice_segments']['atl06_quality_summary']



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
    ON """ + table_name + """ (gtl1_delta_time);
    """
    c.execute(sqlite_str)
    conn.commit()
    conn.close()
    #eventually build time/space/elevation index # Preformace speedup
    print('Index built on table ' + table_name + ' named idx_time')


index_sqlite(sqlite_db, table_name)



#
sqlite_file = 'gt1l_test.sqlite'    # name of the sqlite database file
# table_name = 'my_table_1'  # name of the table to be created
# new_field = 'my_1st_column' # name of the column
# field_type = 'INTEGER'  # column data type
#
# # Connecting to the database file
# conn = sqlite3.connect(sqlite_file)
# c = conn.cursor()
#
#
# # Creating a second table with 1 column and set it as PRIMARY KEY
# # note that PRIMARY KEY column must consist of unique values!
# c.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
#         .format(tn=table_name, nf=new_field, ft=field_type))
#
# # Committing changes and closing the connection to the database file
# conn.commit()
# conn.close()
