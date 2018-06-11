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
    Secondary x-axis? Better format?
    Slider for timestamp (datetime)?
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
import numpy as np

'''Directories and Files'''
# Home directory
home_dir = os.path.expanduser(r'~/Documents/ZHAW/MAIN/04_Big_Data/30_Project/')
# Stock market data directory
stock_dir = r'spark-stock-market-streaming/collected_data/'
# Tweet data directory
tweet_dir = r'???'

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
def transDF(df, stamp, header):
    '''Pre-processing of the data by doing some transformations
    and aggregations to the dataframe created previously
    Args:
        df(dataframe): input dataframe
        stamp(str): name column to parse for datetime
        header(dict): a dictionary with {'original col name':'new col name'}
    return:
        df(dataframe): transformed dataframe'''
    df['stamp'] = pd.to_datetime(df[stamp])
    df['year'] = df['stamp'].dt.year
    df['month'] = df['stamp'].dt.month
    df['day'] = df['stamp'].dt.day
    df['time'] = df['stamp'].dt.time
    # Drop unneeded columns
    df.drop([stamp], axis=1, inplace=True)
    # Set dataframe index
    df.set_index(['year', 'month', 'day', 'time'], inplace=True)
    # Rename columns
    df.rename(columns=header, inplace=True)
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    df.sort_index(inplace=True)
    return df

'''Get and pre-process data'''
dfs = buildDF(home_dir, stock_dir, 'Stock Quotes')
dfss = transDF(dfs, '4. timestamp',
               {'1. symbol':'sym',
                '2. price':'$', 
                '3. volume':'vol'}
              )

available_stocks = dfss['sym'].unique()

''' Dash '''
app = dash.Dash()

app.layout = html.Div([
        
        html.Div([
                
            html.H1('ZHAW, CAS Machine Intelligence'),
            
            html.H2('Big Data Project'), 
            
            html.H3('Data Analysis'),
            
            html.Div([dcc.Dropdown(
                                    id='stock_selection',
                                    options=[{'label':i, 'value':i} for i in available_stocks],
                                    value=[available_stocks[0]],
                                    multi=True
                                    )
                            ],
                            style={'width': '60%', 'align':'left'}),
            
            html.Div([dcc.Graph(id='stock_graph')]), 
            
                ])
            ])

# Dash CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
# Loading screen CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})

@app.callback(
        Output(component_id='stock_graph', component_property='figure'),
        [Input(component_id='stock_selection', component_property='value')],
        )

def update_stock_graph(value):
    # List of traces
    traces = []
    # Loop the available stocks to produce a trace for each stock
    for i in range(len(value)):
        df_stock = dfss[dfss['sym'] == value[i]]
        X = df_stock.index.get_level_values('time')
        Y = df_stock['$']
        # Create an individual trace
        trace = go.Scatter(x = X,
                           y = Y,
                           mode = 'lines+markers',
                           name = value[i])
        # Append to the list of traces
        traces.append(trace)
        
    title_stock = 'Stock Market Price'
    
    stock_fig = {'data':traces,
                 'layout': go.Layout(title=title_stock,
                           xaxis=dict(title='Timestamp',
                                      tickformat='%H:%M:%S',
                                      tickmode='auto',
                                      nticks=10),
                           yaxis=dict(title='Price'),
                           hovermode='closest')
                    }

    return stock_fig

if __name__ == '__main__':
    app.run_server(debug=True)