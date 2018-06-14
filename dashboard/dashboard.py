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
    Average price per day chart
    Add Summary of data to the top of the app
    Drop rows on tweets for timestamps out of stock time range, or adjust x-axis range
    For tweets, plot count of tweets per minute?
    Scatter log follower count vs stock price?
    Histogram or bars instead of line for log_follower_count grouped by
    Input for size of round, 5 Min, 10 Min, etc
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
# Windows workstation
if os.name == 'nt':
    home_dir = r'//userhome/users$/ksagilop/home/ZHAW/MAIN/04_Big_Data/'
    stock_dir = r'spark-stock-market-streaming/collected_data/'
    tweet_dir = r'spark-stock-market-streaming/collected_tweets_csv_raw/'
# Linux workstation
if os.name == 'posix':
    home_dir = os.path.expanduser(r'~/Documents/ZHAW/MAIN/04_Big_Data/30_Project/')
    stock_dir = r'spark-stock-market-streaming/collected_data/'
    tweet_dir = r'spark-stock-market-streaming/collected_tweets_csv_raw/'

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

''' Build Dataframe from csv file '''
def buildDFbis(base_dir, data_dir):
    '''Construct a big dataframe by aggregating the individual csv files
    located at the proper data directory
    Args:
        base_dir(str), the home or base directory
        data_dir(str), the directory containing the data
    return:
        df(dataframe), full dataframe iaw csv structure'''
    folder = os.path.join(base_dir + data_dir)
    files = os.listdir(folder)
    file_path = os.path.join(folder + files[0])
    # Dataframe
    df = pd.read_csv(file_path,
                     sep=';',
                     parse_dates={'stamp':['timestamp']},
                     keep_date_col=False)
    df['date'] = df['stamp'].dt.date
    df['time'] = df['stamp'].dt.time
    df.set_index(['date', 'time'], inplace=True)
    return df

'''Load data'''
dfs = buildDF(home_dir, stock_dir, 'Stock Quotes')
dft = buildDFbis(home_dir, tweet_dir)

''' Data Preprocessing '''
dfss = transDF(dfs, stamp_name, header_names, stock_names)

''' Additional Transformations '''
# Min and max timestamps on stock data
stock_hr_min = dfss.index.get_level_values('time').min()
stock_hr_max = dfss.index.get_level_values('time').max()
# Construct masks and select data
mask_hr_min = dft.index.get_level_values('time') > stock_hr_min
dft = dft.loc[mask_hr_min]
mask_hr_max = dft.index.get_level_values('time') < stock_hr_max
dft = dft.loc[mask_hr_max]

''' Create Controls '''
available_stocks = dfss['sym'].unique()
available_dates = dfss.index.get_level_values('date').unique()
available_times = dfss.index.get_level_values('time').unique()
min_time = dfss.index.get_level_values('time').min()
max_time = dfss.index.get_level_values('time').max()
available_tags = dft.hashtag.unique()

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
                                value=[available_stocks[0]],
                                multi=True
                                    )
                    ],
                    style={'width':'60%'},
                    className='row'
                            ),
    
            html.Div(
                [
                    html.P('Select Hashtag(s):'),
                    dcc.Dropdown(
                                id='tag_selection',
                                options=[{'label':i, 'value':i} for i in available_tags],
                                value=[available_tags[0]],
                                multi=True
                                    )
                    ],
                    style={'width':'45%', 'display':'inline-block'},
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
                    style={'width':'15%', 'display':'inline-block'}),
            
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
         Input(component_id='date_selection', component_property='value'),
         Input(component_id='tag_selection', component_property='value')
         ],
        )

def update_stock_graph(stock_sel, date_sel, tag_sel):
    # List of traces
    traces = []
    # Reduce the dataframe to the day selected
    df_day = dfss.loc[date_sel]
    dft_day = dft.loc[date_sel]
    # Round time to aggregate tweets
    dft_day['stamp_round'] = dft_day.stamp.dt.round('1Min')
    # Loop the available stocks to produce a trace for each stock
    for i in range(len(stock_sel)):
        df_stock = df_day[df_day['sym'] == stock_sel[i]]
        #X = df_stock.index.get_level_values('time') # Get from multiindex
        X = df_stock['stamp']
        Y = df_stock['$']
        # Create an individual trace
        trace = go.Scatter(x = X,
                           y = Y,
                           mode = 'lines+markers',
                           name = stock_sel[i])
        # Append to the list of traces
        traces.append(trace)
    
    # Loop the available tags to produce a trace for each tag
    for i in range(len(tag_sel)):
        df_tweet = dft_day[dft_day['hashtag'] == tag_sel[i]]
        
        Y_tweet = df_tweet.groupby(['stamp_round'])['log_followers_count'].sum()
        X_tweet = Y_tweet.index
        
        # Create an individual trace
        trace_tweet = go.Bar(x = X_tweet,
                                 y = Y_tweet,
                                 name = tag_sel[i],
                                 opacity=0.5,
                                 yaxis='y2')
        # Append to the list of traces
        traces.append(trace_tweet)
        
    title_stock = 'Stock Price & Tweets'
    
    stock_fig = {'data':traces,
                 'layout': go.Layout(title=title_stock,
                           xaxis=dict(title='Timestamp (Hour)',
                                      type='date',
                                      tickmode='auto',
                                      nticks=12,
                                      tickformat='%H:%M'
                                          ),
                           yaxis=dict(title='Price'),
                           yaxis2=dict(title='Log Follower count',
                                       overlaying='y',
                                       side='right'
                                           ),
                           hovermode='closest')
                    }

    return stock_fig

if __name__ == '__main__':
    app.run_server(debug=True)