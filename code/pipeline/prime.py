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

db = pymongo.MongoClient(
    f"mongodb+srv://example:{os.getenv('MONGO_ATLAS_PW')}@cluster0.b2q7e.mongodb.net/?retryWrites=true&w=majority"
)

# PRIME
# Bulk create date dimension
date1 = "1970-01-01"
date2 = date.today().strftime("%Y-%m-%d")
date_df = pd.date_range(date1, date2)
d = {
    "date": date_df.strftime("%Y-%m-%d").to_list(),
    "year": date_df.year.to_list(),
    "quarter": date_df.quarter.to_list(),
    "month": date_df.month.to_list(),
    "day": date_df.day.to_list(),
    "dayofyear": date_df.dayofyear.to_list(),
    "weekday": date_df.weekday.to_list(),
    "is_month_end": date_df.is_month_end,
    "is_quarter_start": date_df.is_quarter_start,
    "is_quarter_end": date_df.is_quarter_end,
    "is_leap_year": date_df.is_leap_year,
}

for day in pd.DataFrame(data=d).to_dict("record"):
    db.datawarehouse.dates.replace_one({"date": day["date"]}, day, upsert=True)

# Create index(es)
db.datawarehouse.dates.create_index([ ("date", -1) ])
