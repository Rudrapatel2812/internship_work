import pandas as pd
import numpy as np
from datetime import date,datetime
from functools import reduce
import re
import warnings
warnings.filterwarnings('ignore')

import os
import shutil

pd.set_option('display.max_column',None)
pd.set_option('display.max_row',None)
pd.options.mode.chained_assignment = None

basePath = '/Users/patelrudra/Documents/Tally Data April 23 To May-23/'
data=pd.read_excel(basePath + "Sales/Sales-072.xls",skiprows=3)


def function(data):
    data=data.drop([0])
    data=data.drop(['Unnamed: 2',"Unnamed: 3"], axis=1)
    data['Vch No.']=data['Vch No.'].fillna(method='ffill')
    grouped_df = data.groupby('Vch No.').agg(lambda x: x.to_list()).reset_index()
    x = pd.DataFrame(grouped_df['Particulars'].to_list())
    x["on"]=grouped_df["Vch No."]
    y=pd.DataFrame(grouped_df['Vch Type'].to_list())
    y["on"]=grouped_df["Vch No."]
    a=pd.DataFrame(grouped_df['Credit'].to_list())
    a["on"]=grouped_df["Vch No."]
    z=pd.DataFrame(grouped_df['Debit'].to_list())
    z["on"]=grouped_df["Vch No."]
    b=pd.DataFrame(grouped_df['Date'].to_list())
    b["on"]=grouped_df["Vch No."]
    data_frames = [x, y, z,a,b]
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['on'],
                                                how='outer'), data_frames)
    return df_merged;                                            

data4=function(data)
data4.to_csv('/Users/patelrudra/Documents/Tally Data April 23 To May-23/f.csv')