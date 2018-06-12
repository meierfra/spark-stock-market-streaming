#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  9 16:05:47 2018

@author: @pandastrail

Data visualization with Dash
ZHAW CAS Machine Intelligence
Big Data Project

Create dataframes based on the json files located at the proper folder
Transform and aggregate dataframes as needed
Callback Dash Layouts and visualize the data 

ToDo
    Secondary x-axis? Better format? Spacing?
    Add Summary of data to the top of the app
"""
import pandas as pd
import dash
from pandas.io.json import json_normalize
import json
import os
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import datetime as dt

'''Directories and Files'''
# Home directory
home_dir = os.path.expanduser(r'~/Documents/ZHAW/MAIN/04_Big_Data/30_Project/')
# Stock market data directory
stock_dir = r'spark-stock-market-streaming/collected_data/'
# Tweet data directory
tweet_dir = r'???'

'''Data Features '''
# Stock Names
stock_names = {'GS':'Goldman Sachs Group Inc',
               'LMT':'Lockheed Martin Corporation',
               'TSLA':'TESLA',
               'MSFT':'Microsoft Corporation',
               'AAPL':'Apple Inc.',
               'MCD':'McDonalds Corporation',
               'NKE':'Nike Inc',
               'PFE':'Pfizer Inc.',
               'FB':'Facebook, Inc.',
               'GOOGL':'Alphabet Inc.'
              }

# Name of timestamp to parse 
stamp_name = '4. timestamp'

# Header names
header_names = {'1. symbol':'sym',
                '2. price':'$', 
                '3. volume':'vol'}

'''Build Dataframes from json files'''
def buildDF(base_dir, data_dir, json_col):
    '''Construct a big dataframe by aggregating the individual json files
    located at the proper data directory
    Args:
        base_dir(str), the home or base directory
        data_dir(str), the directory containing the data
        json_col(str), which column to normalize from the json file
    return:
        df(dataframe), full dataframe iaw json structure'''
    folder = os.path.join(base_dir + data_dir)
    files = os.listdir(folder)
    count_files = 0
    for file in files:
        file_path = os.path.join(folder + file)
        with open(file_path) as data_file:
            data = json.load(data_file)
            if count_files == 0:
                df = json_normalize(data, json_col)
                count_files += 1
            else:
                df_temp = json_normalize(data, json_col)
                df = df.append(df_temp, ignore_index=True)
                count_files += 1
    return df

'''Dataframe pre-processing'''
def transDF(df, stamp, header, names):
    '''Pre-processing of the data by doing some transformations
    and aggregations to the dataframe created previously
    Args:
        df(dataframe): input dataframe
        stamp(str): name column to parse for datetime
        header(dict): a dictionary with {'original col name':'new col name'}
    return:
        df(dataframe): transformed dataframe'''
    df['stamp'] = pd.to_datetime(df[stamp])
    df['date'] = df['stamp'].dt.date
    df['time'] = df['stamp'].dt.time
    # Drop unneeded columns
    df.drop([stamp], axis=1, inplace=True)
    # Set dataframe index
    df.set_index(['date', 'time'], inplace=True)
    # Rename columns
    df.rename(columns=header, inplace=True)
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    # Sort Index
    df.sort_index(inplace=True)
    # Rename rows
    for key, value in names.items():
        mask = df.sym == key
        df.loc[mask, 'sym'] = value + ', ' + '(' + key + ')'
    return df

'''Load data'''
dfs = buildDF(home_dir, stock_dir, 'Stock Quotes')

''' Data Preprocessing '''
dfss = transDF(dfs, stamp_name, header_names, stock_names)

''' Create Controls '''
available_stocks = dfss['sym'].unique()
available_dates = dfss.index.get_level_values('date').unique()
available_times = dfss.index.get_level_values('time').unique()
min_time = dfss.index.get_level_values('time').min()
max_time = dfss.index.get_level_values('time').max()

''' Dash '''
app = dash.Dash()

app.layout = html.Div([
        
        html.Div([
                
            html.H2('ZHAW, CAS Machine Intelligence'),
            
            html.H3('Big Data Project, Data Visualization'), 
            
            html.Div(
                [
                    html.P('Select Stock(s):'),
                    dcc.Dropdown(
                                id='stock_selection',
                                options=[{'label':i, 'value':i} for i in available_stocks],
                                value=[],
                                multi=True
                                    )
                    ],
                    style={'width':'100%', 'display':'inline-block'},
                    className='row'
                            ),
    
            html.Div(
                [
                    html.P('Select Date:'),
                    dcc.Dropdown(
                                id='date_selection',
                                options=[{'label':i, 'value':i} for i in available_dates],
                                value=[],
                                multi=False
                                    ),
                    ],
                    style={'width':'25%', 'display':'inline-block'}),
            
            html.Div([dcc.Graph(id='stock_graph')]), 
            
                ])
            ])

# Dash CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
# Loading screen CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})

@app.callback(
        Output(component_id='stock_graph', component_property='figure'),
        [Input(component_id='stock_selection', component_property='value'),
         Input(component_id='date_selection', component_property='value')
         ],
        )

def update_stock_graph(stock_sel, date_sel):
    # List of traces
    traces = []
    # Reduce the dataframe to the day selected
    df_day = dfss.loc[date_sel]
    # Loop the available stocks to produce a trace for each stock
    for i in range(len(stock_sel)):
        df_stock = df_day[df_day['sym'] == stock_sel[i]]
        X = df_stock.index.get_level_values('time')
        Y = df_stock['$']
        # Create an individual trace
        trace = go.Scatter(x = X,
                           y = Y,
                           mode = 'lines+markers',
                           name = stock_sel[i])
        # Append to the list of traces
        traces.append(trace)
        
    title_stock = 'Stock Market Price'
    
    stock_fig = {'data':traces,
                 'layout': go.Layout(title=title_stock,
                           xaxis=dict(title='Timestamp',
                                      tickmode='auto',
                                      nticks=10,
                                      tickformat='%H:%M'
                                      ),
                           yaxis=dict(title='Price'),
                           hovermode='closest')
                    }

    return stock_fig

if __name__ == '__main__':
    app.run_server(debug=True)