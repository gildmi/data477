#!/usr/bin/env python3

import csv
import os
import time

import numpy as np
import pandas as pd
import pymongo
import requests
import yfinance as yf

from datetime import date
from pprint import pprint


LIMIT = 1000
sp500_file = "constituents.csv"
date_format = "%Y-%m-%d %H:%M:%S"
sp500_list = []
sp500_info_dict = {}
sp500_sma_dict = {}


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

# TRANSFORM
# stock_info
info_field = [
    "symbol",
    "sector",
    "fullTimeEmployees",
    "longBusinessSummary",
    "city",
    "state",
    "country",
    "industry",
    "trailingAnnualDividendYield",
    "averageDailyVolume10Day",
    "trailingAnnualDividendRate",
    "averageVolume10days",
    "dividendRate",
    "exDividendDate",
    "trailingPE",
    "regularMarketVolume",
    "marketCap",
    "averageVolume",
    "priceToSalesTrailing12Months",
    "forwardPE",
    "fiveYearAvgDividendYield",
    "dividendYield",
    "exchange",
    "shortName",
    "longName",
    "quoteType",
    "market",
    "enterpriseToRevenue",
    "profitMargins",
    "forwardEps",
    "sharesOutstanding",
    "sharesShort",
    "sharesPercentSharesOut",
    "lastSplitDate",
    "lastSplitFactor",
    "lastDividendDate",
]
for r in sp500_list:
    symbol = r[0]
    print(symbol)
    info = db.datalake.stock_info.find_one({"symbol": symbol})
    sp500_info_dict[symbol] = {}
    # Only get wanted fields
    for f in info_field:
        if f in info:
            sp500_info_dict[symbol][f] = info[f]
        else:
            sp500_info_dict[symbol][f] = None
            print("MISSING", symbol, f)
    # Convert epoch dates to date strings
    if "lastSplitDate" in info:
        sp500_info_dict[symbol]["lastSplitDate"] = time.strftime(
            date_format, time.localtime(sp500_info_dict[symbol]["lastSplitDate"])
        )
    if "lastDividendDate" in info:
        sp500_info_dict[symbol]["lastDividendDate"] = time.strftime(
            date_format, time.localtime(sp500_info_dict[symbol]["lastDividendDate"])
        )
    if "exDividendDate" in info:
        sp500_info_dict[symbol]["exDividendDate"] = time.strftime(
            date_format, time.localtime(sp500_info_dict[symbol]["exDividendDate"])
        )
    # Add last updated
    sp500_info_dict[symbol]["last_updated"] = date.today().strftime(date_format)


# LOAD
# Stock info
for i, t in enumerate(sp500_list):
    symbol = t[0]
    if i < LIMIT:
        db.datawarehouse.stock_info.replace_one(
            {"symbol": symbol}, sp500_info_dict[symbol], upsert=True
        )

db.datawarehouse.stock_info.create_index([ ("symbol", -1) ])

# VALIDATE
# Makes sure everything was updated 
# Double check counts
# Look for negative amounts
# Check dates make sense and are defined in our dates table/collection