# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas
import os
import psycopg2
from datetime import datetime
from datetime import timedelta
#
#
psql_h = os.getenv("psql_h")
psql_u = os.getenv("psql_u")
psql_p = os.getenv("psql_p")
psql_settings = "host={} user={} password={}".format(psql_h,psql_u,psql_p)
#
#
def fetch_main(region,ts_1,ts_2):
    return pandas.read_sql(
	"select * from data where region like '{}' and time_stamp between '{}' and '{}' \
	order by time_stamp desc".format(region,ts_1,ts_2)
	,conn)
#
#
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()

app = dash.Dash(__name__)

colors = {
    'background': '#efefefff',
    'text': '#38761d'
}
#
current_time=pandas.to_datetime(datetime.now()).round(freq = 'T')
#
main = fetch_main('NE','2020-01-01 00:00:00',current_time)
#
#
app.layout = html.Div([
    html.H1('Grid Insights'),
    html.Div([
        html.P("The table below contains the latest data:")
    ])
,
html.Label('Select grid region'),
    dcc.Dropdown(id='region_drop',
        options=[
	    {'label': 'California', 'value': 'CAL'},
            {'label': 'Carolinas', 'value': 'CAR'},
            {'label': 'Central', 'value': 'CENT'},
            {'label': 'Florida', 'value': 'FLA'},
            {'label': 'Mid-Atlantic', 'value': 'MIDA'},
            {'label': 'Midwest', 'value': 'MIDW'},
            {'label': 'New England', 'value': 'NE'},
            {'label': 'Northwest', 'value': 'NW'},
            {'label': 'New York', 'value': 'NY'},
            {'label': 'Southeast', 'value': 'SE'},
            {'label': 'Southwest', 'value': 'SW'},
            {'label': 'Tennessee', 'value': 'TEN'},
            {'label': 'Texas', 'value': 'TEX'}
        ],
        value='NE'
    ),
html.Label('Select year'),
    dcc.Dropdown(id='year_drop',
	options=[
	    {'label': '2020', 'value': '2020'},
            {'label': '2019', 'value': '2019'},
            {'label': '2018', 'value': '2018'},
            {'label': '2017', 'value': '2017'},
            {'label': '2016', 'value': '2016'},
            {'label': '2015', 'value': '2015'}
	],
	value='2020'
    ),
 dash_table.DataTable(
    id='table',
    value='NE',
    columns=[{"name": i, "id": i} for i in main.columns],
    data=main.to_dict("rows")
)]
)

#@app.callback(
#    Output('table', 'table'),
#    [Input('region_drop', 'value')])
def update_table(region,ts_1,ts_2):
    
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True, port='8080')
#
#
cur.close()
conn.close()
#
###################################################################################################
