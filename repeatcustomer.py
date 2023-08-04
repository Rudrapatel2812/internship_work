#!/usr/bin/env python
# coding: utf-8
import sys
import pandas as pd 
import numpy as np
import re
import os
from dateutil.relativedelta import relativedelta
import logging
import calendar
from datetime import date, datetime,timedelta
pd.set_option('display.max_column',None)
pd.set_option('display.max_row',None)

# basePath = sys.argv[1]


basePath = '/Users/patelrudra/Documents/Medkart/all 3 month 5 jun/'
outputfilename = date.today().strftime("%Y%m%d")


logger = logging.getLogger('my_logger')
logging.basicConfig(filename=basePath+'logs/repeatCustomer.log', filemode='w', level=logging.DEBUG)

def Detail_clean(df):
    df = df[(df['AlternateStoreCode'].notnull())&(df['StoreCode'].notnull())]
    
    df = df[df['BillSeries'].isin(['SC','SR'])]
    df['Amount'] = df['Amount'].astype('float64')
    df['Quantity'] = df['Quantity'].astype('int64')
    df['AlternateStoreCode'] = df['AlternateStoreCode'].replace('HO',1)
    df = df[df['AlternateStoreCode'].notnull()]
    df = df[(df['AlternateStoreCode']!='HO')&(df['AlternateStoreCode']!='MKWS')]
    df[['AlternateStoreCode','StoreCode','ProductCode']] = df[['AlternateStoreCode','StoreCode','ProductCode']].astype('int64')
    df['BillNumber'] = df['BillNumber'].astype('str')
    df['Amount'] = df['Amount'].astype('float64')
    return df


try:
    currentMonth = int(date.today().strftime('%m'))
    #/Users/patelrudra/Documents/Medkart/all 3 month 5 jun/monthwisesalesdump
    df1 = pd.read_csv(basePath + "monthwisesalesdump/WS_" +  ((date.today()-timedelta(days=-1)) + relativedelta(months=-4)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
    df1['BillMonth']='1/'+pd.to_datetime(df1['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    df1 = df1[df1['BillSeries']=='SC']

    df2 = pd.read_csv(basePath+"monthwisesalesdump/WS_" + ((date.today()-timedelta(days=-1)) + relativedelta(months=-3)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
    df2['BillMonth']='1/'+pd.to_datetime(df2['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    df2 = df2[df2['BillSeries']=='SC']

    df3 = pd.read_csv(basePath+"monthwisesalesdump/WS_" + ((date.today()-timedelta(days=-1)) + relativedelta(months=-2)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
    df3['BillMonth']='1/'+pd.to_datetime(df3['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    df3 = df3[df3['BillSeries']=='SC']

    df4 = pd.read_csv(basePath+"salesDetails.csv",skiprows=5,low_memory=False)
    df4['BillMonth']='1/'+pd.to_datetime(df4['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    df4 = df4[df4['BillSeries']=='SC']
    df4 = Detail_clean(df4)

    df4['BillMonth']='1/'+pd.to_datetime(df4['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    

    WS = pd.concat([df1,df2,df3,df4],axis=0)
    WS = Detail_clean(WS)

    MIS  =pd.read_csv(basePath+"input/mismapping.csv")
    
    MIS = MIS[MIS['mislabel'].isin(['Generic Medicine','Private Labels'])]


    WS = WS.merge(MIS.loc[:,['productcode','mislabel']],left_on='ProductCode', right_on='productcode', how='left').drop('productcode', 1)
    #print(WS.head())
    WS['Gen_Sale'] = np.where(WS['mislabel'].isin(['Generic Medicine','Private Labels']),WS['Amount'],0)
   
    WS = WS[WS['BillSeries']=='SC']


    WS['AlternateStoreCode'] = WS['AlternateStoreCode'].replace('HO',1)
    WS['AlternateStoreCode'] = WS['AlternateStoreCode'].astype('int64')
    WS = WS.loc[:,['BillNumber','SalesmanName','CustomerName','CustomerCode','AlternateStoreCode','BillDate','Amount','BillMonth','Gen_Sale']]
    WS2 = WS.rename(columns={'SalesmanName':'UserName'})

    WS2['AlternateStoreCode'] = WS2['AlternateStoreCode'].replace('HO',1)
    WS2['BillDate'] = pd.to_datetime(WS2.BillDate,format='%d/%m/%Y')
    WS2['BillMonth'] = pd.to_datetime(WS2['BillMonth'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
    WS2['BillMonth'] = pd.to_datetime(WS2['BillMonth'],format='%m/%Y').dt.date

    month  = WS2['BillMonth'].unique()
    #print(month)
    WS3 = WS2.groupby(['BillDate','BillMonth','AlternateStoreCode','UserName','CustomerCode']).agg({'Amount':'sum','BillNumber':'nunique','Gen_Sale':'sum'}).reset_index()

    WS3  = WS3[WS3['AlternateStoreCode']!='MKWS']
    WS3['AlternateStoreCode'] = WS3['AlternateStoreCode'].astype('int64')
    WS3['CustomerCode'] = WS3['CustomerCode'].astype('int64')
    WS3['Month'] =  WS3['BillMonth']



    l1 = []
    for m in month:
        df1 = WS3[WS3['Month']==m]
        df1 = df1.groupby(['Month','BillDate','AlternateStoreCode','UserName','CustomerCode','BillNumber']).agg({'Amount':'sum','Gen_Sale':'sum'}).reset_index()
        df2 = WS3[WS3['Month'].isin([m+ relativedelta(months=-1),m+ relativedelta(months=-3),m+relativedelta(months=-2)])]
        
        if df2['Month'].nunique()==3:
            df3 = df2.groupby(['AlternateStoreCode','CustomerCode','BillNumber']).agg({'Amount':'sum'}).reset_index()
            df3 = df3.drop_duplicates(subset=['AlternateStoreCode','CustomerCode'])
            df3['Key'] = 1
            df4 = df1.merge(df3,on=['CustomerCode','AlternateStoreCode'],how='left',suffixes=(None,'_X'))
            df4['Repeat_Sale'] = np.where(df4['Key']==1.0,df4['Amount'],np.nan)
            df4['Repeat_Bills'] = np.where(df4['Key']==1.0,df4['BillNumber'],np.nan)
            df4['Repeat_GenSale'] = np.where(df4['Key']==1.0,df4['Gen_Sale'],np.nan)
            df5 = df4.groupby(['Month','BillDate','AlternateStoreCode','UserName']).agg({'Key':'count','Amount':'sum','Repeat_Sale':'sum','Repeat_Bills':'count','Repeat_GenSale':'sum'}).reset_index()
            l1.append(df5)
            
    #print(l1.head())
    df5 = pd.concat(l1)
    
    df5=  df5.drop_duplicates(subset = ['BillDate','AlternateStoreCode','UserName'])
    df5 = df5.groupby(['Month','BillDate','AlternateStoreCode','UserName']).agg({'Amount':'sum','Repeat_Sale':'sum','Repeat_Bills':'sum','Repeat_GenSale':'sum'}).reset_index()
    df5.columns = ['month','billdate','alternatestorecode','username','amount','repeatsale','repeatbills','repeatgenericsale']
    df5['billdate'] = pd.to_datetime(df5['billdate'],format='%Y-%m-%d').dt.date

    #df5.to_csv(basePath + "output/repeatcustomer.csv" ,index=False)
    print(df5.head())
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" +  str(e))
    logging.error("Line Number:" + str(exc_tb.tb_lineno))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")

