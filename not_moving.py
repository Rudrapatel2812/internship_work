import pandas as pd
import numpy as np
from datetime import date,datetime
from functools import reduce
import re
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_column',None)
pd.set_option('display.max_row',None)
pd.options.mode.chained_assignment = None


basePath = '/Users/patelrudra/Documents/Medkart/all 3 month 5 jun/monthwisesalesdump/'

WS_Dec = pd.read_csv(basePath+"WS_Feb_2023_Detail.csv",skiprows=5,low_memory=False)
WS_Dec['BillMonth'] = pd.to_datetime(WS_Dec['BillDate'], format='%d/%m/%Y').dt.month
WS_Dec = WS_Dec[WS_Dec['AlternateStoreCode']!='MKWS']
WS_Dec['AlternateStoreCode'] = WS_Dec['AlternateStoreCode'].replace('HO',1)
WS_Dec['AlternateStoreCode'] = WS_Dec['AlternateStoreCode'].astype('int64')

WS_Jan = pd.read_csv(basePath+"WS_Mar_2023_Detail.csv",skiprows=5,low_memory=False)
WS_Jan['BillMonth'] = pd.to_datetime(WS_Jan['BillDate'], format='%d/%m/%Y').dt.month
WS_Jan = WS_Jan[WS_Jan['AlternateStoreCode']!='MKWS']
WS_Jan['AlternateStoreCode'] = WS_Jan['AlternateStoreCode'].replace('HO',1)
WS_Jan['AlternateStoreCode'] = WS_Jan['AlternateStoreCode'].astype('int64')

WS_Feb = pd.read_csv(basePath+"WS_Apr_2023_Detail.csv",skiprows=5,low_memory=False) 
WS_Feb['BillMonth'] = pd.to_datetime(WS_Feb['BillDate'], format='%d/%m/%Y').dt.month
WS_Feb = WS_Feb[WS_Feb['AlternateStoreCode']!='MKWS']
WS_Feb['AlternateStoreCode'] = WS_Feb['AlternateStoreCode'].replace('HO',1)
WS_Feb['AlternateStoreCode'] = WS_Feb['AlternateStoreCode'].astype('int64')

WS_Apr = pd.read_csv(basePath+"WS_May_2023_Detail.csv",skiprows=5,low_memory=False)
WS_Apr['BillMonth'] = pd.to_datetime(WS_Apr['BillDate'], format='%d/%m/%Y').dt.month
WS_Apr = WS_Apr[WS_Apr['AlternateStoreCode']!='MKWS']
WS_Apr['AlternateStoreCode'] = WS_Apr['AlternateStoreCode'].replace('HO',1)
WS_Apr['AlternateStoreCode'] = WS_Apr['AlternateStoreCode'].astype('int64')


WS_Sales = pd.concat([WS_Dec,WS_Jan,WS_Feb,WS_Apr], axis=0)
WS_Sales = WS_Sales[WS_Sales['BillSeries']=='SC']
WS_Sales = WS_Sales.loc[:,['BillDate','Quantity','ProductCode','AlternateStoreCode','BillMonth']]

WS_Sales['Quantity'] = WS_Sales['Quantity'].astype('int64')
WS_Sales = WS_Sales[WS_Sales['Quantity']>0]

#print(WS_Sales['BillMonth'].value_counts())

WS_tb = WS_Sales.groupby(['AlternateStoreCode','ProductCode','BillMonth']).agg({'Quantity':'sum'}).unstack().reset_index()

WS_tb = WS_tb.droplevel(level=1,axis=1)
WS_tb.columns = ['AlternateStoreCode','ProductCode','Jan_Qty','Feb_Qty','Mar_Qty','Apr_Qty']

WS_tb[['AlternateStoreCode','ProductCode']] = WS_tb[['AlternateStoreCode','ProductCode']].astype('int64')
combine = WS_tb.copy()


combine['L4M_Freq'] = (combine.loc[:,['Jan_Qty','Feb_Qty','Mar_Qty','Apr_Qty']]>0).sum(axis=1)
combine['Avg_Qty'] = round((combine[combine.loc[:,['Jan_Qty','Feb_Qty','Mar_Qty','Apr_Qty']]>0]).mean(axis=1),2)

combine['NM_Category'] =np.where(combine['L4M_Freq']==0,'Non-Moving','Moving')
#combine['MM_Category'] =np.where(combine['MinStock'].isnull(),'Non-MinMax','MinMax')
combine = combine[combine['AlternateStoreCode']!=1]

#combine['ExcessStock'] = np.where((combine['Stock']-2*combine['MaxStock'])>0,combine['Stock']-2*combine['MaxStock'],np.nan)
print(combine.head())

# df = pd.read_csv("/Users/patelrudra/Downloads/Stock Report Batchwise (2).csv", skiprows=5)
# df = df[(df['AlternateStoreCode']!='MKWS')&(df['AlternateStoreCode']!='RW')&(df['AlternateStoreCode']!='Supplier')]

# df = df.loc[:,['StoreCode','StoreName','AlternateStoreCode','ProductCode','ProductName','BatchDescription','ExpiryDate','Department','Class','Stock','ItemCost']]
# df['AlternateStoreCode'] = df['AlternateStoreCode'].replace('HO',1)
# df['AlternateStoreCode'] = df['AlternateStoreCode'].astype('int64')

# store_df = df.loc[:,['AlternateStoreCode','StoreName','ProductCode','ProductName','Stock','ItemCost']]
# store_df['CostVal'] = store_df['ItemCost']*store_df['Stock']
# store_df = store_df.groupby(['AlternateStoreCode','StoreName','ProductCode','ProductName']).agg({'Stock':'sum','CostVal':'sum'}).reset_index()
# print(store_df.head())

  
 