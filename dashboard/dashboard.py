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
    Other scatter charts?
    Add processing time logs for functions and loops; and app launch
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
import plotly
import datetime as dt

''' Version '''
print('plotly ', plotly.__version__)
print('pandas ', pd.__version__)

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
                '2. price':'price_str', 
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
    print('Loading json files...')
    for file in files:
        file_path = os.path.join(folder + file)
        with open(file_path) as data_file:
            data = json.load(data_file)
            if count_files == 0:
                df = json_normalize(data, json_col)
                count_files += 1
            else:
                try:
                    df_temp = json_normalize(data, json_col)
                    df = df.append(df_temp, ignore_index=True)
                    count_files += 1
                except:
                    print(file, 'normalize failed')
                    continue
    print(count_files, ' json files read')
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
    # Drop original timestamp column
    df.drop([stamp], axis=1, inplace=True)
    # Set dataframe index
    df.set_index(['date', 'time'], inplace=True)
    # Rename columns
    df.rename(columns=header, inplace=True)
    # dtype change for price
    df['price'] = pd.to_numeric(df['price_str'])
    df.drop(['price_str'], axis=1, inplace=True)
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
# Length of data
stock_len = len(dfs)
tweet_len = len(dft)
summary_one = ('Summary: ', str(stock_len), ' Stock data points collected. ', 
               str(tweet_len), ' Tweets processed.')
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
            
            html.Div([dcc.Markdown(
                '''
                ### ZHAW, CAS Machine Intelligence
                Data Visualization,
                [Big Data Project](https://github.com/meierfra/spark-stock-market-streaming).
                '''.replace('  ', ''),
                className='eight columns offset-by-two'
                        )
                    ],
                className='row',
                style={'text-align': 'center', 'margin-bottom': '15px'}
                    ),
            
            html.P(summary_one),
            
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
                    style={'width':'50%', 'display':'inline-block'},
                    className='row'
                            ),
            
            html.Div(
                [
                    html.P(''),
                    ],
                    style={'width':'5%', 'display':'inline-block'},
                    className='row'
                            ),
                
            html.Div(
                [
                    html.P('Select Date:'),
                    dcc.Dropdown(
                                id='date_selection',
                                options=[{'label':i, 'value':i} for i in available_dates],
                                value=available_dates[-1],
                                multi=False
                                    ),
                    ],
                    style={'width':'15%', 'display':'inline-block'}),
    
            html.Div(
                [
                    html.P('Select Twitter Hashtag(s):'),
                    dcc.Dropdown(
                                id='tag_selection',
                                options=[{'label':i, 'value':i} for i in available_tags],
                                value=[],
                                multi=True
                                    )
                    ],
                    style={'width':'35%', 'display':'inline-block'},
                    className='row'
                            ),
            
            html.Div(
                [
                    html.P(''),
                    ],
                    style={'width':'5%', 'display':'inline-block'},
                    className='row'
                            ),
                        
            html.Div(
                [
                    html.P('Tweet Aggregation:'),
                    dcc.Slider(
                            id='twagg_selection',
                            min=1,
                            max=30,
                            step=None,
                            marks={
                                    1: '1 Min',
                                    5: '5 Min',
                                    10: '10 Min',
                                    15: '15 Min',
                                    30: '30 Min'
                                            },
                            value=5
                                )
                        ],
                        style={'width':'30%', 'display':'inline-block'}),
            
            html.Div([dcc.Graph(
                                id='stock_graph',
                                style={'height': '60vh'})
                                ]), 
            
            html.Div([dcc.Markdown(
                id='markdown_below',
                className='eight columns offset-by-two'
                        )
                    ],
                className='row',
                style={'text-align': 'center', 'margin-bottom': '15px'}
                    ),
                ])
            ])

app.css.append_css({
    'external_url': (
        'https://cdn.rawgit.com/chriddyp/0247653a7c52feb4c48437e1c1837f75'
        '/raw/a68333b876edaf62df2efa7bac0e9b3613258851/dash.css'
    )
})

if 'DYNO' in os.environ:
    app.scripts.append_script({
        'external_url': 'https://cdn.rawgit.com/chriddyp/ca0d8f02a1659981a0ea7f013a378bbd/raw/e79f3f789517deec58f41251f7dbb6bee72c44ab/plotly_ga.js'  # noqa: E501
    })


@app.callback(
        Output(component_id='stock_graph', component_property='figure'),
        [Input(component_id='stock_selection', component_property='value'),
         Input(component_id='date_selection', component_property='value'),
         Input(component_id='tag_selection', component_property='value'),
         Input(component_id='twagg_selection', component_property='value')
         ],
        )

def update_stock_graph(stock_sel, date_sel, tag_sel, twagg_sel):
    print('update_stock_graph function input:')
    print('stock_selection: ', stock_sel, type(stock_sel))
    print('date_selection: ', date_sel, type(date_sel))
    print('tag_selection: ', tag_sel, type(tag_sel))
    print('twagg_selection: ', twagg_sel, type(twagg_sel))
    print('---------------------------------')
    # Parse Tweet aggregation
    twagg = str(twagg_sel) + 'Min'
    # List of traces
    traces = []
    # Reduce the dataframe to the day selected
    df_day = dfss.loc[date_sel]
    dft_day = dft.loc[date_sel]
    # Round time to aggregate tweets
    dft_day['stamp_round'] = dft_day.stamp.dt.round(twagg)
    # Loop the available stocks to produce a trace for each stock
    for i in range(len(stock_sel)):
        df_stock = df_day[df_day['sym'] == stock_sel[i]]
        #X = df_stock.index.get_level_values('time') # Get from multiindex
        X = df_stock['stamp']
        Y = df_stock['price']
        #Y_max = Y.max()
        #Y_max_xloc = X[Y == Y.max()]
        #Y_mean = Y.mean()
        # Create an individual trace
        trace = go.Scatter(x = X,
                           y = Y,
                           mode = 'lines+markers',
                           name = stock_sel[i])
        # Append to the list of traces
        traces.append(trace)
    
    # Loop the available tags to produce a trace for each tag
    for i in range(len(tag_sel)):
        # Filter dataframe for selected hashtag
        df_tweet = dft_day[dft_day['hashtag'] == tag_sel[i]]
        
        # Get total number of tweets
        #cnt_sum = df_tweet.cnt.sum()
        
        # Bar chart data
        Y_tweet = df_tweet.groupby(['stamp_round'])['log_followers_count'].sum()
        X_tweet = Y_tweet.index
        #Y_max_tweet = Y_tweet.max()
        
        # Create an individual trace
        trace_tweet = go.Bar(x = X_tweet,
                             y = Y_tweet,
                             name = tag_sel[i],
                             opacity=0.5,
                             yaxis='y2')
        # Append to the list of traces
        traces.append(trace_tweet)
        
    #title_stock = 'Mean Stock Price: ' + str(Y_mean) + '\n' + '#  of Tweets: ' + str(cnt_sum) 
    title_stock = 'Stock Price & Twitter'
    
    # TESLA annotation - placing is not optimal
    tesla_note = 'Layoff announcement!'
    if stock_sel == ['TESLA, (TSLA)'] and date_sel == '2018-06-12':
        note = [dict(x='2018-06-12 12:50:00',
                     y=360,
                     xref='x',
                     yref='y',
                     text=tesla_note,
                     showarrow=True,
                     font=dict(
                             family='Courier New, monospace',
                             size=12,
                             color='#234481'
                                 ),
                     align='center',
                     arrowhead=2,
                     arrowsize=1,
                     arrowwidth=2,
                     arrowcolor='#636363',
                     ax=20,
                     ay=-30,
                         )
                    ]
    else:
        note = []
    
    stock_fig = {'data':traces,
                 'layout': go.Layout(title=title_stock,
                                     xaxis=dict(title='Timestamp (Hour)',
                                                type='date',
                                                tickmode='auto',
                                                nticks=12,
                                                tickformat='%H:%M'
                                                    ),
                                     yaxis=dict(title='Price'),
                                     yaxis2=dict(title='Tweet Log Follower Count',
                                                 overlaying='y',
                                                 showline=False,
                                                 showgrid=False,
                                                 side='right'
                                                     ),
                                     hovermode='closest'
                                         )
                            }

    return stock_fig

@app.callback(
        Output(component_id='markdown_below', component_property='children'),
        [Input(component_id='stock_selection', component_property='value'),
         Input(component_id='date_selection', component_property='value')
         ],
        )

def update_markdown(stock_sel, date_sel):
    if stock_sel == ['TESLA, (TSLA)'] and date_sel == '2018-06-12':
        markdown_text = '''
                        Tesla's CEO Elon Musk made a layoff  
                        [announcement](https://www.cnbc.com/2018/06/12/tesla-to-cut-about-9-percent-of-jobs-across-company-report.html).
                        on June 12th, 2018 around noon.
                        '''.replace('  ', ''),
    else:
        markdown_text = ''
    
    return markdown_text

if __name__ == '__main__':
    app.run_server(debug=True)