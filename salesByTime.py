#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd

from datetime import date,datetime,time,timedelta
import re
import calendar
import logging
import warnings
import sys
warnings.filterwarnings("ignore")

pd.set_option('display.max_column',None)
pd.set_option('display.max_row',None)


# In[2]:

basePath = '/Users/patelrudra/Documents/Medkart/may data/'

# basePath = sys.argv[1]
outputfilename = date.today().strftime("%Y%m%d")


# logger = logging.getLogger('my_logger')
# logging.basicConfig(filename=basePath+'logs/salesByTime.log', filemode='w', level=logging.DEBUG)
# In

try:
    def Detail_clean(df):
        df.columns = [x.lower() for x in df.columns]
        df = df[(df['alternatestorecode'].notnull())&(df['storecode'].notnull())]
        
        df = df[df['billseries'].isin(['SC','SR'])]
        df['amount'] = df['amount'].astype('float64')
        df['quantity'] = df['quantity'].astype('int64')
        df['alternatestorecode'] = df['alternatestorecode'].replace('HO',1)
        df = df[df['alternatestorecode'].notnull()]
        df = df[(df['alternatestorecode']!='HO')&(df['alternatestorecode']!='MKWS')]
        df[['alternatestorecode','storecode','productcode']] = df[['alternatestorecode','storecode','productcode']].astype('int64')
        df['billnumber'] = df['billnumber'].astype('str')
        df['amount'] = df['amount'].astype('float64')
        df['salesmanname'] = df['salesmanname'].fillna(0)
        return df


    # In[3]:


    Jan_Detail = pd.read_csv(basePath+"salesDetails.csv",skiprows=5,low_memory=False)

    DS_Abs = Detail_clean(Jan_Detail)


    # In[4]:


    df   = Detail_clean(DS_Abs)


    # In[5]:


    import datetime as dt
    df['billtime'] = pd.to_datetime(df['billtime'], format =  '%I:%M%p')
    #print(df["billtime"].head())
    # df['billtime'] = df['billtime'].apply(lambda x : x.strftime("%H:%M:%S"))



    import time
    import datetime as dt
    df['billdate']=pd.to_datetime(df['billdate'],format='%d/%m/%Y').dt.date
    df['billtime'] = pd.to_datetime(df['billtime'], format =  '%H:%M%p')
    #print(df["billtime"].head(60))
    df['cat'] = pd.cut(df['billtime'].dt.hour,
                    bins = [0,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24],
                    labels=['Before 8','8-9 hr','9-10 hr','10-11 hr','11-12 hr', '12-13 hr', '13-14 hr','14-15 hr', '15-16 hr','16-17 hr','17-18 hr',
                            '18-19 hr','19-20 hr','20-21 hr','21-22 hr','22-23 hr','After 23'],
                        right= False)


    # In[7]:

    df = df.groupby(['alternatestorecode','billdate','cat']).agg({'billnumber':'nunique','amount':'sum'}).reset_index()



    # In[ ]:


    df.to_csv(basePath + "output/hourlydata1.csv" ,index=False)
    #print(df.head())

except Exception as e:
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" +  str(e))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")

