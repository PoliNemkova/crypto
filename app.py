import bitfinex
import pandas as pd
import numpy as np
import datetime
import time
import os
import matplotlib.pyplot as plt
import csv

f = open('./WHO-COVID-19-global-data.csv')
csv_f = csv.reader(f)

# for row in csv_f:
#   print (row)

def fetch_data(start=1364767200000, stop=1545346740000, symbol='btcusd', interval='1m', tick_limit=1000, step=60000000):
  # Create api instance
  api_v2 = bitfinex.bitfinex_v2.api_v2()

  data = []
  start = start - step
  while start < stop:
    start = start + step
    end = start + step
    res = api_v2.candles(symbol=symbol, interval=interval, limit=tick_limit, start=start, end=end)
    data.extend(res)
    print('Retrieving data from {} to {} for {}'.format(pd.to_datetime(start, unit='ms'),
                                                        pd.to_datetime(end, unit='ms'), symbol))
    time.sleep(1.5)
  return data