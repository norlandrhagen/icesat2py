import pandas as pd
import numpy as np
import bokeh
import subset
from bokeh.plotting import figure, output_file, show, save
from bokeh.models import ColumnDataSource, HoverTool,LinearColorMapper
color_list = ['lightcoral','crimson','cornflowerblue','royalblue','lightgreen','seagreen']


sqlite_db = 'icesat2_test.sqlite'    # name of the sqlite database file
table_name = 'ATL06'
beam = 'gt1l'

var_list = ['x_atc','h_li']
startDate = '2019-02-01 00:00'
endDate = '2019-02-02 00:30'
minLat = 63
maxLat = 66
minLon = -26
maxLon = -12



df = subset.spaceTime(sqlite_db, table_name, beam,  startDate, endDate, minLat, maxLat, minLon, maxLon)
df = df[df[beam+'_atl06_quality_summary']==0]
y_min, y_max =(np.min(df[beam+'_h_li'])-2),(np.max(df[beam+'_h_li'])+2)


p = figure(title=table_name + '_' + beam, x_axis_label='x_atc', y_axis_label='h_li',y_range=(y_min,y_max),sizing_mode='stretch_both')



p.scatter(df[beam+'_x_atc'], df[beam+'_h_li'],color = color_list[0], size=10, fill_alpha=0.7,legend=beam)

# show the results
show(p)
