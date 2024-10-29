import gc
import os
import sys
import csv
import datetime
import argparse
import happybase
import pandas as pd
import multiprocessing as mp
from functools import partial

key = ['a_', 'b_', 'c_', 'd_', 'e_', 'f_', 'g_']
columns = [b'session_stay:creation_time', b'session_stay:page_id', b'session_stay:url_hostname', b'session_stay:url(decode)']

# arg
description = """
Get data from hbase
ex: python Hbase.py 2018-08-01 2018-08-02
grasp data between 2018-08-01 and 2018-08-02, if you just want to get data for one day, please input the same date twice
The data would be saved in csv
"""

parser = argparse.ArgumentParser(description=description)
parser.add_argument("-start", help="ex: 2018-08-01, inclusive")
parser.add_argument("-end", help="ex: 2018-08-01, inclusive")
args = parser.parse_args()

# # connect to hbase
# pool = happybase.ConnectionPool(1, host='xxx.xxx.xx.xxx', port=8888)
# with pool.connection() as connection:
# 	table = connection.table(b'footprint')

# date range
def date_range(start_date:str, end_date:str):
	day = []
	start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
	end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
	period = (end - start).days + 1
	while period:
		day.append(datetime.datetime.strftime(start, '%Y-%m-%d'))
		start += datetime.timedelta(1)
		period -= 1
	return day

# get one data
def one_get(day, key):
	pool = happybase.ConnectionPool(1, host='xxx.xxx.xx.xxx', port=8888)
	with pool.connection() as connection:
		table = connection.table(b'session_stay')
		scanner = table.scan(row_prefix=f'{key}{day}'.encode(), columns=columns)
		multi_datalist = []
		for _, data in scanner:
			multi_datalist.append(data)
	return multi_datalist

# multiprocess
def multi_get()
	for days in day:
		with mp.Pool() as p:
			data = p.map(partial(one_get, days), key)
			p.terminate()
			all_data = [k for i in data for k in i]
			foot = pd.DataFrame.from_daysct(all_data)
			foot.columns = [i.decode('utf-8') for i in foot.columns]
			foot.columns = foot.columns.map(lambda x: x.split(':')[1])
			foot = foot.fillna(b'0')
			foot = foot.applymap(lambda x: x.decode('utf-8'))
			foot.to_csv(f'./hbase/session_{days}.csv.gz', index=False, quoting=csv.QUOTE_ALL, compression='gzip')