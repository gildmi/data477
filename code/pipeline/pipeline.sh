#! /usr/bin/env bash

# SETUP
# Get S&P 500
wget https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv

# CAPTURE
python3 capture.py

# PRIME DW
python3 prime.py

# ETL
python3 etl.py

# CALCULATIONS
python3 agg.py

# RUN STATISTICS

# FEATURE ENGINEER
# TRAIN MODELS
# TEST MODELS
# DEPLOY MODELS
