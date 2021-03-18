#!/usr/bin/env python3

import csv
import json
import os
import time

import pymongo
import requests
import yfinance as yf

from datetime import date
from pprint import pprint


LIMIT = 1000
sp500_file = 'constituents.csv'
date_format = '%Y-%m-%d %H:%M:%S'
sp500_list = []
sp500_dict = {}
sp500_sma_dict = {}
sp500_history_dict = {}

# Read in S&P500 list
if os.path.exists(sp500_file):
    with open(sp500_file, 'r') as f:
        sp500_reader = csv.reader(f)
        for i, row in enumerate(sp500_reader):
            if i != 0:
                sp500_list.append(row)
else:
    print(f"ERROR!!! Cannot find {sp500_file}")
    exit(-1)


# Connect to db
db = pymongo.MongoClient(f"mongodb+srv://example:{os.getenv('MONGO_ATLAS_PW')}@cluster0.b2q7e.mongodb.net/?retryWrites=true&w=majority")
today = date.today().strftime(date_format)


# Load stock_info raw data into data lake
for i, r in enumerate(sp500_list):
    symbol = r[0]
    if i < LIMIT:  # limit
        sp500_dict[symbol] = yf.Ticker(symbol.replace(".","-")).info
        sp500_dict[symbol]["_last_updated"] = today
        time.sleep(5)
        db.datalake.stock_info.replace_one({"symbol":symbol}, sp500_dict[symbol], upsert=True)

# Validate data lake count
num_docs = db.datalake.stock_info.count_documents({})
if num_docs < LIMIT or num_docs > len(sp500_list):
    print(f"ERROR!! Number of docs in stock_info is {num_docs}")


# Load SMA raw data into data lake
for i, r in enumerate(sp500_list):
    symbol = r[0]
    if i < LIMIT:  # limit
        url = f"https://www.alphavantage.co/query?function=SMA&symbol={symbol}&interval=weekly&time_period=10&series_type=open&apikey={os.getenv('ALPHA_VANTAGE_API_KEY')}"
        r = requests.get(url)
        if r.status_code == 200:
            sp500_sma_dict[symbol] = json.loads(r.content.decode("utf-8").replace('\n',''))
        else:
            print(f"ERROR!!! status {r.status_code} for {url}")
        sp500_sma_dict[symbol]["_last_updated"] = today
        db.datalake.stock_sma.replace_one({"Meta Data":{"1: Symbol": symbol}}, sp500_sma_dict[symbol], upsert=True)
        time.sleep(15)

# Validate data lake count
num_docs = db.datalake.stock_sma.count_documents({})
if num_docs < LIMIT or num_docs > len(sp500_list):
    print(f"ERROR!! Number of docs in stock_sma is {num_docs}")


# Load stock history raw data into data lake
for i, r in enumerate(sp500_list[399:]):
    print(i, r)
    symbol = r[0]
    if i < LIMIT:  # limit
        sp500_history_dict[symbol] = yf.Ticker(symbol.replace(".","-")).history(period="max")
        sp500_history_dict[symbol].index = sp500_history_dict[symbol].index.strftime("%Y-%m-%d")
        sp500_history_dict[symbol] = sp500_history_dict[symbol].to_dict("index")
        rec = {"symbol":symbol, "data":sp500_history_dict[symbol]}
        db.datalake.stock_history.replace_one({"symbol":symbol}, rec, upsert=True)

# Validate data lake count
num_docs = db.datalake.stock_history.count_documents({})
if num_docs < LIMIT or num_docs > len(sp500_list):
    print(f"ERROR!! Number of docs in stock_history is {num_docs}")

# Create index(es)
db.datalake.stock_info.create_index([ ("symbol", -1) ])
db.datalake.stock_history.create_index([ ("symbol", -1) ])