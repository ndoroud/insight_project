#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 13:00:54 2020

@author: nima
"""
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
project_dir = os.getenv("project_dir")
#
psql_h = os.getenv("psql_h")
psql_u = os.getenv("psql_u")
psql_p = os.getenv("psql_p")
psql_settings = "host={} user={} password={}".format(psql_h,psql_u,psql_p)
#
#
current_time=pandas.to_datetime(datetime.now()).round(freq = 'T')
#
#
def fetch_main(region,ts_1='2020-01-01 00:00:00',ts_2=current_time):
    return pandas.read_sql(
	"select * from data where region like '{}' and time_stamp between '{}' and '{}' \
	order by time_stamp desc".format(region,ts_1,ts_2)
	,conn)
#
#
def fetch_history(region,ts_1='2020-01-01 00:00:00',ts_2=current_time):
    return pandas.read_sql(
	"select * from data_history where region like '{}' and time_stamp between '{}' and '{}' \
	order by time_stamp desc".format(region,ts_1,ts_2)
	,conn)
#
#
score = {'CAL': 11.756066453881754,
 'CAR': 9.48010756943678,
 'CENT': 8.323718267015566,
 'FLA': 9.027733823114657,
 'MIDA': 2.2269317051276136,
 'MIDW': 2.6134167490821323,
 'NE': 14.15539560229558,
 'NY': 10.503888497715101,
 'NW': 6.875541327042233,
 'SE': 8.385228273451107,
 'SW': 31.761667161128578,
 'TEN': 12.429833967438077,
 'TEX': 6.104764876958678}
#
#
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()
#
#
s19 = pandas.read_sql("select * from summary2019 order by net_generation_solar desc",conn)
#
#
colors = {
    'background': '#efefefff',
    'text': '#38761d'
}
#
app = dash.Dash(__name__)

app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([html.H1('Grid Insights'),
    dcc.Link('Sources', href='/sources'),
    html.Br(),
    dcc.Link('2019 Scores', href='/score'),
    html.Br(),
    dcc.Link('Browse the data', href='/data'),
    html.Br(),
    html.H1('2019 anual summary (per capita)'),
dash_table.DataTable(
    id='summary_table',
    columns=[{"name": "Region", "id": "region"},
             {"name": "Demand (MWh)", "id": "demand"},
             {"name": "Net generation (MWh)", "id": "net_generation"},
             {"name": "Net generation solar (MWh)", "id": "net_generation_solar"},
             {"name": "GHI (W/m^2)", "id": "ghi"},
             {"name": "DNI (W/m^2)", "id": "dni"},
             {"name": "", "id": "windspeed"}],
    style_table={'overflowX': 'scroll'},
    style_cell={
        'height': 'auto',
        # all three widths are needed
        'minWidth': '60px', 'width': '100px', 'maxWidth': '150px',
        'whiteSpace': 'normal', 'textAlign': 'left'
    },
    data=s19.to_dict("rows")
    )
])

page_1_layout = html.Div([html.H1('Grid Insights'),
    dcc.Link('Home', href='/home'),
    html.Br(),
    dcc.Link('Summary 2019', href='/summary'),
    html.Br(),
    dcc.Link('Browse the data', href='/data'),
    html.Br(),
    dcc.Link("EIA's hourly grid monitor", href='https://www.eia.gov/beta/electricity/gridmonitor/dashboard/electric_overview/US48/US48')
    ])

page_2_layout = html.Div([html.H1('Grid Insights'),
    dcc.Link('Sources', href='/sources'),
    html.Br(),
    dcc.Link('Home', href='/home'),
    html.Br(),
    dcc.Link('Browse the data', href='/data'),
    dcc.Graph(
        id='2019 scores',
        figure={
            'data': [
                {'x': list(score.keys()), 'y': list(score.values()), 'type': 'bar', 'name': 'score'}
            ],
            'layout': {
                'title': 'Viability score'
            }
        }
    )])

page_3_layout = html.Div([
    dcc.Link('Sources', href='/sources'),
    html.Br(),
    dcc.Link('Summary 2019', href='/summary'),
    html.Br(),
    dcc.Link('Home', href='/home'),
    html.Br(),
    html.Label('Filter by grid region'),
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
            {'label': 'Texas', 'value': 'TEX'},
            {'label': 'All', 'value': '%'}
        ],
        value='%'
    ),
    html.Div(id='my-div'),
    dcc.DatePickerRange(
        id='date-range',
        min_date_allowed=datetime(2015, 9, 1),
        max_date_allowed=pandas.to_datetime(datetime.now()).round(freq = 'H').to_pydatetime(),
        initial_visible_month=datetime(2020, 2, 1),
        start_date=(pandas.to_datetime(datetime.now()).round(freq = 'H') - timedelta(days=1)).to_pydatetime(),
        end_date=pandas.to_datetime(datetime.now()).round(freq = 'H').to_pydatetime()
    ),
    html.Div(id='output-container-date-picker-range'),
dash_table.DataTable(
    id='data_table',
    columns=[{"name": "Region", "id": "region"},
             {"name": "Time Stamp (UTC)", "id": "time_stamp"},
             {"name": "Demand (MWh)", "id": "demand"},
             {"name": "Net generation (MWh)", "id": "net_generation"},
             {"name": "Net generation solar (MWh)", "id": "net_generation_solar"},
             {"name": "GHI (W/m^2)", "id": "ghi"},
             {"name": "DNI (W/m^2)", "id": "dni"},
             {"name": "Windspeed (m/s)", "id": "windspeed"},
             {"name": "Last Updated", "id": "updated"}],
    style_table={'overflowX': 'scroll'},
    style_cell={
        'height': 'auto',
        # all three widths are needed
        'minWidth': '60px', 'width': '100px', 'maxWidth': '150px',
        'whiteSpace': 'normal', 'textAlign': 'left'
    },
    style_data_conditional=[{
        "if": {'filter_query': "{region} eq 'NE'"},
        "backgroundColor": "#3D9970",
        'color': 'white'
    }],
    data=[]
    )

])

@app.callback(
    dash.dependencies.Output('data_table', 'data'),
    [dash.dependencies.Input('region_drop', 'value'),
     dash.dependencies.Input('date-range', 'start_date'),
     dash.dependencies.Input('date-range', 'end_date'),]
)
def update_table(value,start_date, end_date):
    return fetch_main(value,start_date,end_date).to_dict("rows")


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/sources':
        return page_1_layout
    elif pathname == '/score':
        return page_2_layout
    elif pathname == '/data':
        return page_3_layout
    else:
        return index_page
    
if __name__ == '__main__':
    app.run_server(debug=True)
#
#
cur.close()
conn.close()
#
###############################################################################