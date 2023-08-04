################################################################### Import Required packages #########################################################################

import numpy as np
import pandas as pd
import os
from datetime import date,datetime,time,timedelta
import re
import calendar
import logging
import warnings
import sys
warnings.filterwarnings("ignore")

################################################################### Required Sales Detail  Files #########################################################################

#basePath = 'D:/OneDrive - Medkart Pharmacy Pvt. Ltd/Dhaval/Python_ipynb/Data etl/'
basePath='/Users/patelrudra/Documents/MedKart/'
#/Users/patelrudra/Documents/MedKart
# basePath = sys.argv[1]

detailInputFile = basePath+"salesDetails.csv"
absInputFile = basePath+"salesAbstract.csv"
outputfilename = date.today().strftime("%Y%m%d")

Details = pd.read_csv(detailInputFile,skiprows=5,low_memory=False)

def Detail_clean(df):
    df.columns = [x.lower() for x in df.columns]
    df = df[(df['alternatestorecode'].notnull())&(df['storecode'].notnull())]
    df['billdate']=pd.to_datetime(df['billdate'],format='%d/%m/%Y')
    
    df = df[df['billseries'].isin(['SC','SR'])]
    df['amount'] = df['amount'].astype('float64')
    df['quantity'] = df['quantity'].astype('int64')
    df['alternatestorecode'] = df['alternatestorecode'].replace('HO',1)
    df = df[df['alternatestorecode'].notnull()]
    df = df[(df['alternatestorecode']!='HO')&(df['alternatestorecode']!='MKWS')]
    df[['alternatestorecode','storecode','productcode']] = df[['alternatestorecode','storecode','productcode']].astype('int64')
    df['billnumber'] = df['billnumber'].astype('str')
    df['Amount'] = df['amount'].astype('float64')
    df['salesmanname'] = df['salesmanname'].fillna(0)
    
    return df

################################################################### Required Sales Abstract  Files #########################################################################
DS_Abs = pd.read_csv(absInputFile,skiprows=5,low_memory=False)

def Abs_clean(df):
    df.columns = [x.lower() for x in df.columns]
    df = df[(df['alternatestorecode'].notnull())&(df['storecode'].notnull())]
    df['billdate']=pd.to_datetime(df['billdate'],format='%d/%m/%Y')
    df = df[df['billseries'].isin(['SC','SR'])]
    df = df[df['storename'].notnull()]
    df = df[df['billseries']=='SC']
    df = df.rename(columns={'new customer type':'newcustomertype'})
    df = df.loc[:,['storecode','billdate','customercode','newcustomertype','billnumber','amount']]
    return df

################################################################### Clean And Merge Detail and Abstract File #########################################################################

# logger = logging.getLogger('my_logger')
# logging.basicConfig(filename=basePath+'logs/scoreCard.log', filemode='w', level=logging.DEBUG)

try:

    Details = Detail_clean(Details)
    DS_Abs = Abs_clean(DS_Abs)

    Returns = Details[Details['billseries']=='SR']
    Details = Details[Details['billseries']=='SC']
    Details['salesmanname'] = Details['salesmanname'].astype('str')

    Details = Details[Details['amount']>0]
    Details = Details.loc[:,['alternatestorecode','storecode','billdate','customercode','billnumber','salesmanname','productcode','productname','batch','promotionname','quantity','amount','tax','department','costofsale','basevalue']]
    
    Details = Details.rename(columns={'salesmanname':'username'})
    Details['quantity'] = np.where(Details['productcode']==3905,0,Details['quantity'])
    Details1 = Details.groupby(['billdate','alternatestorecode','storecode','username','billnumber','productcode','productname','department']).agg({'amount':'sum','basevalue':'sum','costofsale':'sum'}).reset_index()

    ################################################################### Dummy Bills Output #########################################################################

    DS_Abs['amount'] = DS_Abs['amount'].astype('float64')
    DS_Abs = DS_Abs[DS_Abs['amount']>0]
    DS_Abs = DS_Abs.drop('amount',1)
    DS_Abs = DS_Abs.drop_duplicates(subset=['storecode','billdate','billnumber'])

    # In[6]:

    DS_Abs['storecode'] = DS_Abs['storecode'].astype('int64')
    DS_Abs['billnumber'] = DS_Abs['billnumber'].astype('str') 

    # In[7]:

    Details = pd.merge(Details,DS_Abs,on=['storecode','billdate','billnumber'],how='left')

    ################################################################### New Customer Sale #########################################################################

    Details['newcustcode'] = np.where(Details['newcustomertype']=='NEW',Details['customercode_x'],np.nan)

    # In[9]:

    Details['newcustbill'] = np.where(Details['newcustomertype']=='NEW',Details['billnumber'],np.nan)
    Details['newcustsale'] = np.where(Details['newcustomertype']=='NEW',Details['amount'],np.nan)

    ################################################################### Offer Sales #########################################################################

    df14 = Details[(Details['billdate']>=datetime(2023,5,1))&(Details['billdate']<=datetime(2023,5,31))]

    df14['offersale'] = np.where((df14['promotionname'].str.contains('SCHEME'))&(df14['productcode'].isin([9944,4535,15828,18686,18746,18542,18707,9947])),df14['amount'],np.nan)

    df14['qty'] = np.where((df14['promotionname'].str.contains('SCHEME'))&(df14['productcode'].isin([9944, 9947])),df14['quantity']/5,df14['quantity'])
    df14['qty'] = np.where((df14['promotionname'].str.contains('SCHEME'))&(df14['productcode']==15828),df14['quantity']/2,df14['qty'])

    # df14['qty'] = df14['quantity']

    df14['offerqty'] = np.where((df14['promotionname'].str.contains('SCHEME'))&(df14['productcode'].isin([9944,4535,15828,18686,18746,18542,18707,9947])),df14['qty'],np.nan)
    df14['offerqty'] = np.where((df14['promotionname'].str.contains('Disc 2 Perc'))&(df14['productcode'].isin([18542])),df14['qty'],df14['offerqty'])

    Details = df14.copy()

    Details['username'] = Details['username'].fillna(0)
    Details1 = Details.groupby(['billdate','alternatestorecode','storecode','username','billnumber','productcode','productname']).agg({'amount':'sum','offersale':'sum','offerqty':'sum','newcustbill':'nunique','newcustsale':'sum','qty':'sum'}).reset_index()
    Details1['newcustbill'] = Details1['newcustbill'].replace(0,np.nan)

    ################################################################### MIS MAPPING #########################################################################

    MIS_Map = pd.read_csv(basePath+"input/mismapping.csv")
    MIS_Map = MIS_Map.loc[:,['productcode','mislabel']]
    Details = Details.merge(MIS_Map,on ='productcode',how='left')
    Details['mislabel'] =Details['mislabel'].fillna('0')


    Details['newcustgnrccat'] =np.where(((Details['mislabel'].str.contains('Generic'))|(Details['mislabel'].str.contains('Private') )),Details['newcustcode'],np.nan)
    Details['newcustgenericsale'] = np.where( Details['newcustgnrccat'].notnull(),Details['amount'],np.nan)


    ################################################################### Generic & OTC Sales #########################################################################

    Details['gensale'] = np.where((Details['mislabel'].str.contains('Generic'))|(Details['mislabel'].str.contains('Private')),Details['amount'],np.nan)
    Details['genqty'] = np.where((Details['mislabel'].str.contains('Generic'))|(Details['mislabel'].str.contains('Private')),Details['qty'],np.nan)

    Details['brandedotcsale'] = np.where(Details['mislabel']=='Branded OTC',Details['amount'],np.nan)
    Details['brandedotcqty'] = np.where(Details['mislabel']=='Branded OTC',Details['qty'],np.nan)

    Details['genotcsale'] = np.where(Details['mislabel']=='Generic OTC',Details['amount'],np.nan)
    Details['genotcqty'] = np.where(Details['mislabel']=='Generic OTC',Details['qty'],np.nan)

    Details['genmedsale'] = np.where(Details['mislabel'].isin(['Generic Medicine','Private Label']),Details['amount'],np.nan)
    Details['genmedqty'] = np.where(Details['mislabel'].isin(['Generic Medicine','Private Label']),Details['qty'],np.nan)

    Details['brndmedsale'] = np.where(Details['mislabel'].isin(['Branded Direct Medicine','Branded Medicine']),Details['amount'],np.nan)
    Details['brndmedqty'] = np.where(Details['mislabel'].isin(['Branded Direct Medicine','Branded Medicine']),Details['qty'],np.nan)

    Details['otcsale'] = np.where(Details['mislabel'].str.contains('OTC'),Details['amount'],np.nan)
    Details['otcqty'] = np.where(Details['mislabel'].str.contains('OTC'),Details['qty'],np.nan)



    Chronic = pd.read_csv(basePath + "input/chroniclist.csv")
    Chronic['productcode'] = Chronic['productcode'].astype('int64')
    Chronic = Chronic.drop_duplicates(subset='productcode')


    Details = pd.merge(Details,Chronic.loc[:,['productcode','acutechronic']],on='productcode',how='left')
    Details['acutechronic'].fillna('Acute',inplace=True)

    ################################################################### Acute & Chronic Sales And Customers #########################################################################

    NewChronic = Details[Details['acutechronic']=='Chronic']['newcustcode'].unique()
    NewChronic = NewChronic[1:]

    NewAcute = Details[Details['acutechronic']=='Chronic']['newcustcode'].unique()
    NewAcute = NewChronic[1:]


    Details['newchroniccust'] = np.where((Details['newcustomertype']=='NEW')&(Details['newcustcode'].isin(NewChronic)),Details['newcustcode'],np.nan)
    Details['newacutecust'] = np.where((Details['newcustomertype']=='NEW')&(Details['acutechronic']=='Acute')&(Details['newchroniccust'].isnull()),Details['newcustcode'],np.nan)
    Details['newchronicgenrccust'] = np.where((Details['newchroniccust'].notnull())&(Details['newcustgnrccat'].notnull()),Details['newcustcode'],np.nan)

    Details['newchroniccustsale'] = np.where((Details['newcustomertype']=='NEW')&(Details['newcustcode'].isin(NewChronic)),Details['newcustsale'],np.nan)
    Details['newacutecustsale'] = np.where((Details['newcustomertype']=='NEW')&(Details['acutechronic']=='Acute')&(Details['newchroniccust'].isnull()),Details['newcustsale'],np.nan)
    Details['newchronicgenrccustsale'] = np.where((Details['newchroniccust'].notnull())&(Details['newcustgnrccat'].notnull()),Details['newcustsale'],np.nan)


    Details['quantity'] = np.where(Details['quantity']==3905,0,Details['quantity'])

    params = {
        'amount':'sum',
        'billnumber':'nunique',
        'quantity':'sum',
        'newcustbill':'nunique',
        'newcustsale':'sum',
        'offersale':'sum',
        'offerqty':'sum',
        'brandedotcsale':'sum',
        'brandedotcqty':'sum',
        'brndmedsale':'sum',
        'brndmedqty':'sum',
        'gensale':'sum',
        'genqty':'sum',
        'genotcsale':'sum',
        'genotcqty':'sum',
        'genmedsale':'sum',
        'genmedqty':'sum',
        'otcsale':'sum',
        'otcqty':'sum',
        'carrbsale':'sum',
        'carrbqty':'sum',
        'newcustgnrccat':'nunique',
        'newcustgenericsale':'sum',
        'newchroniccust':'nunique',
        'newacutecust':'nunique',
        'newchronicgenrccust':'nunique',
        'newchroniccustsale':'sum',
        'newacutecustsale':'sum',
        'newchronicgenrccustsale':'sum',
    }

    Offer_OTC  =  Details.copy()
    Offer_OTC['billdate'] = Offer_OTC['billdate'].dt.date
    Offer_OTC = Offer_OTC.loc[:,['billdate','alternatestorecode','username','billnumber','productcode','productname','quantity','amount','offerqty','offersale','otcsale','otcqty','costofsale','basevalue']]
    Offer_OTC.to_csv(basePath + "output/offerotc.csv" ,index=False)

    df = Details.groupby(['billdate','alternatestorecode','storecode','username']).agg(params).reset_index()

    df.to_csv(basePath + "output/usersales.csv",index=False)

    MinMax = pd.read_csv(basePath + "input/allminmax.csv")
    MinMax['month'] = '05-2023'
    MinMax.columns =  [x.lower() for x in MinMax.columns]


    # In[31]:

    MinMax[['storecode','productcode']] = MinMax[['storecode','productcode']].astype('int64')
    MinMax = MinMax[MinMax['minstock']>0]
    MinMax = MinMax.loc[:,['storecode','productcode','month']]
    MinMax['mmkey']=1

    # In[32]:

    MinMax =MinMax.drop_duplicates(subset=['month','productcode','storecode'])

    Details['month'] = Details['billdate'].apply(lambda x: x.strftime('%m-%Y'))

    Details1 = Details.merge(MinMax,on=['storecode','productcode','month'],how='left').drop('month',1)

    Details1['MinMax Sale'] = np.where(Details1['mmkey']==1,Details1['amount'],np.nan)
    Details1['nonminmaxsale'] = np.where((Details1['mmkey'].isnull())|((Details1['mmkey']!=1.0)),Details1['amount'],np.nan)

    # #### Multivitamins

    MV = pd.read_csv(basePath + "input/multivitamin.csv")
    MV['mvkey'] = 1
    MV = MV.loc[:,['productcode','mvkey']]

    # In[46]:

    Details2 = Details1.merge(MV,on='productcode',how='left')

    # In[47]:

    Details2['mvsale'] = np.where(Details2['mvkey']==1.0,Details2['amount'],np.nan)
    Details2['mvqty'] = np.where(Details2['mvkey']==1.0,Details2['qty'],np.nan)


    # OFFER

    # In[48]:
    Details2 = Details2.groupby(['billdate','alternatestorecode','storecode','username','billnumber']).agg({'amount':'sum','offersale':'sum',
                                                                                                        'offerqty':'sum','newcustsale':'sum','offerqty':'sum',
                                                                                                        'carrbsale':'sum','carrbqty':'sum','MinMax Sale':'sum',
                                                                                                        'nonminmaxsale':'sum','otcqty':'sum', 'otcsale':'sum',
                                                                                                        'mvsale':'sum','mvqty':'sum'}).reset_index()
    # In[49]:

    Details2['less150bill'] = np.where(Details2['amount']<150,Details2['billnumber'],np.nan)
    Details2['less300bill'] = np.where((Details2['amount']<=300)&(Details2['amount']>150),Details2['billnumber'],np.nan)
    Details2['less500bill'] = np.where((Details2['amount']<500)&(Details2['amount']>300),Details2['billnumber'],np.nan)
    Details2['gret500bill'] = np.where(Details2['amount']>=500,Details2['billnumber'],np.nan)
    Details2['gret500bill'] = np.where(Details2['amount']>500,Details2['billnumber'],np.nan)

    # In[50]:


    Details3 = Details2.groupby(['billdate','alternatestorecode','username']).agg({'nonminmaxsale':'sum','mvsale':'sum','mvqty':'sum',
                                                                                'less150bill':'nunique','less300bill':'nunique',
                                                                                'less500bill':'nunique','gret500bill':'nunique'}).reset_index()

    dff = pd.merge(df,Details3,on=['billdate','alternatestorecode','username'],how='left')

    dff['billdate'] = dff['billdate'].dt.date

    dff.columns = [x.lower() for x in dff.columns]

    dff.to_csv(basePath + "output/salesdetail.csv" ,index=False)

    Returns = Returns.loc[:,['alternatestorecode','storecode','billdate','billnumber','salesmanname','productcode','productname','batch','promotionname','quantity','amount','tax','department']]

    Returns = Returns.groupby(['billdate','alternatestorecode','salesmanname']).agg({'billnumber':'nunique','amount':'sum'}).reset_index()
    Returns['billdate'] =Returns['billdate'].dt.date

    Returns.columns = [x.lower() for x in Returns.columns]
    Returns.to_csv(basePath + "output/returns.csv" ,index=False)

    print("Finished Executing on " + date.today().strftime("%d - %m - %Y"))
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" +  str(e))
    logging.error("Line Number:" + str(exc_tb.tb_lineno))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")
