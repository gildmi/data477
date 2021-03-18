#!/usr/bin/env python3

import csv
import os
import time
from datetime import date, datetime, timedelta
from pprint import pprint

import numpy as np
import pandas as pd
import pymongo
import requests
import yfinance as yf

LIMIT = 1000
sp500_file = "constituents.csv"
date_format = "%Y-%m-%d %H:%M:%S"
sp500_list = []
sp500_agg_dict = {}


# EXTRACT
# Read in S&P500 list
if os.path.exists(sp500_file):
    with open(sp500_file, "r") as f:
        sp500_reader = csv.reader(f)
        for i, row in enumerate(sp500_reader):
            if i != 0:
                sp500_list.append(row)
else:
    print(f"ERROR!!! Cannot find {sp500_file}")


# Connect to db
db = pymongo.MongoClient(
    f"mongodb+srv://example:{os.getenv('MONGO_ATLAS_PW')}@cluster0.b2q7e.mongodb.net/?retryWrites=true&w=majority"
)

# Get last year
today = datetime.today()
last_year = today - timedelta(days=200)
last_year_list = pd.date_range(last_year, today).strftime("%Y-%m-%d").to_list()

for r in sp500_list:
    symbol = r[0]
    print(symbol)
    info = db.datalake.stock_history.find_one({"symbol": symbol})
    del info["_id"]
    del info["symbol"]
    sp500_agg_dict[symbol] = {}
    for d in info['data']:
        info['data'][d]['date'] = d
    df = pd.DataFrame.from_dict(info['data'], orient='index')
    sp500_agg_dict[symbol]['50_day_avg'] = round(df.tail(50)['Close'].mean(),2)
    sp500_agg_dict[symbol]['200_day_avg'] = round(df.tail(200)['Close'].mean(),2)
    sp500_agg_dict[symbol]['10_day_volume_avg'] = round(df.tail(10)['Volume'].mean(),0)
    sp500_agg_dict[symbol]['dividend_avg'] =round(df[df['Dividends']!=0.0]['Dividends'].mean(),2)
    df2 = df[df['date'].isin(last_year_list)]
    sp500_agg_dict[symbol]['52wk_high'] = round(df2['High'].max(),2)
    sp500_agg_dict[symbol]['52wk_low'] = round(df2['Low'].min(),2)
    sp500_agg_dict[symbol]["last_updated"] = date.today().strftime(date_format)
    sp500_agg_dict[symbol]["symbol"] = symbol

    # VALIDATE
    # Make sure numbers are valid

# Add to data warehouse
for symbol in sp500_agg_dict:
    if i < LIMIT:
        db.datawarehouse.stock_agg.replace_one(
            {"symbol": symbol}, sp500_agg_dict[symbol], upsert=True
        )

# Add index(es)
db.datawarehouse.stock_agg.create_index([ ("symbol", -1) ])

# VALIDATE
# Check for all symbols
# Check to make sure everything was updated