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

basePath = '/Users/patelrudra/Documents/Medkart/all 3 month 5 jun/'


def Detail_clean(df):
    df = df[(df['AlternateStoreCode'].notnull())&(df['StoreCode'].notnull())]
    df = df[df['BillSeries'].isin(['SC','SR'])]
    df['AlternateStoreCode'] = df['AlternateStoreCode'].replace('HO',1)
    df = df[df['AlternateStoreCode'].notnull()]
    df = df[(df['AlternateStoreCode']!='HO')&(df['AlternateStoreCode']!='MKws')]
    return df


df1 = pd.read_csv(basePath+"monthwisesalesdump/ws_" + ((date.today()-timedelta(days=-1)) + relativedelta(months=-4)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
df1["BillMonth"]="1/"+pd.to_datetime(df1["BillDate"],format="%d/%m/%Y").apply(lambda x :x.strftime("%m/%Y"))
df1=df1[df1["BillSeries"]=="SC"]

df2 = pd.read_csv(basePath+"monthwisesalesdump/ws_" + ((date.today()-timedelta(days=-1)) + relativedelta(months=-3)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
df2['BillMonth']='1/'+pd.to_datetime(df2['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
df2 = df2[df2['BillSeries']=='SC']

df3 = pd.read_csv(basePath+"monthwisesalesdump/ws_" + ((date.today()-timedelta(days=-1)) + relativedelta(months=-2)).strftime('%b_%Y') + "_Detail.csv",skiprows=[0,1,2,3,4],low_memory=False)
df3['BillMonth']='1/'+pd.to_datetime(df3['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
df3 = df3[df3['BillSeries']=='SC']

df4=pd.read_csv(basePath + "salesDetails.csv",skiprows=5,low_memory=False)
df4['BillMonth']='1/'+pd.to_datetime(df4['BillDate'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
df4 = df4[df4['BillSeries']=='SC']
df4=Detail_clean(df4)

ws = pd.concat([df1,df2,df3,df4],axis=0)
ws = Detail_clean(ws)

mis=pd.read_csv(basePath+"input/mismapping.csv")
mis=mis[mis["mislabel"].isin(['Generic Medicine','Private Labels'])]
mis2=mis.loc[:,['productcode','mislabel']]

ws=ws.merge(mis2,left_on='ProductCode', right_on='productcode', how='left').drop('productcode', 1)
ws['Gen_Sale'] = np.where(ws['mislabel'].isin(['Generic Medicine','Private Labels']),ws['Amount'],0)


ws = ws.loc[:,['BillNumber','SalesmanName','CustomerName','CustomerCode','AlternateStoreCode','BillDate','Amount','BillMonth','Gen_Sale']]
ws2 = ws.rename(columns={'SalesmanName':'UserName'})

#print(ws3.head())
ws2['BillDate'] = pd.to_datetime(ws2.BillDate,format='%d/%m/%Y')
ws2['BillMonth'] = pd.to_datetime(ws2['BillMonth'],format='%d/%m/%Y').apply(lambda x :x.strftime('%m/%Y'))
ws2['BillMonth'] = pd.to_datetime(ws2['BillMonth'],format='%m/%Y').dt.date

month  = ws2['BillMonth'].unique()

ws3 = ws2.groupby(['BillDate','BillMonth','AlternateStoreCode','UserName','CustomerCode']).agg({'Amount':'sum','BillNumber':'nunique','Gen_Sale':'sum'}).reset_index()

ws3['Month'] =  ws3['BillMonth']
ws3['AlternateStoreCode'] = ws3['AlternateStoreCode'].astype('int64')
ws3['CustomerCode'] = ws3['CustomerCode'].astype('int64')

l1 = []
for m in month:
    df1 = ws3[ws3['Month']==m]
    df1 = df1.groupby(['Month','BillDate','AlternateStoreCode','UserName','CustomerCode','BillNumber']).agg({'Amount':'sum','Gen_Sale':'sum'}).reset_index()
    df2 = ws3[ws3['Month'].isin([m+ relativedelta(months=-2),m+ relativedelta(months=-3),m+relativedelta(months=-1)])]
        
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
df5['billdate'] = pd.to_datetime(df5['billdate'],format='%d-%m-%Y').dt.date
df5.to_csv(basePath + "output/my_repeatcustomer.csv" ,index=False)

#print(df5.head())

    