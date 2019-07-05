""" build sqlite db module"""

"""user input? None? checks in ./data folder for .h5 files. checks if sqlite db named <?> exists
if files exist and db does not exist: create sqllite db with name <?>
for every (hardcoded?) data product. ie laser returns, gridded data, etc... write sqlite table

- for every list h5files:
    import with h5py, write lat/lon/elev/var1/var2/var3/var... to specific data product table
    -create index on space/time for faster subset



"""



import pandas
import h5py
import sqlite3
import glob

inputdir = '/home/nrhagen/Documents/icesat2/Ouputs/'
hdf5_file_list = glob.glob(inputdir + '*.h5*')

def read_hdf5_headers():
    f = h5py.File(hdf5_file_list[0], 'r')
    l1_keys = f.keys()
    return l1_keys

l1_keys = read_hdf5_headers()

sqlite_db = 'icesat2_test.sqlite'    # name of the sqlite database file
table_name = 'gtl1_test'
def build_sqliteDB(sqlite_db, table_name):
    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()


    c.execute("""CREATE TABLE {tn} ({gtl1_dt} {gtl1_dt_ft},
    {gtl1_lat} {gtl1_lat_ft},
    {gtl1_lon} {gtl1_lon_ft},
    {gtl1_h_li} {gtl1_h_li_ft},
    {gtl1_x_atc} {gtl1_x_atc_ft},
    {gtl1_atl_06_qual} {gtl1_atl_06_qual_ft}""".format(tn=table_name,
    gtl1_dt='gtl1_delta_time', gtl1_dt_ft='REAL',
    gtl1_lat='gtl1_lat', gtl1_lat_ft='REAL',
    gtl1_lon='gtl1_lon', gtl1_lon_ft='REAL',
    gtl1_h_li='gtl1_h_li', gtl1_h_li_ft='REAL',
    gtl1_x_atc='gtl1_x_atc', gtl1_x_atc_ft='REAL',
    gtl1_atl_06_qual='gtl1_atl_06_qual', gtl1_atl_06_qual_ft='INT'))

    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()

build_sqliteDB(sqlite_db, table_name)

def import_clean_hdf5():
    pass
def insert_into_sqlite():
    # import sqlite3
    #
    # persons = [
    #     ("Hugo", "Boss"),
    #     ("Calvin", "Klein")
    # ]
    #
    # con = sqlite3.connect(":memory:")
    #
    # # Create the table
    # con.execute("create table person(firstname, lastname)")
    #
    # # Fill the table
    # con.executemany("insert into person(firstname, lastname) values (?,?)", persons)
    pass
def index_sqlite():
    pass


#
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
