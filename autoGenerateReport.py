import boto3
import os
import datetime
import shutil
import logging
from zipfile import ZipFile
from datetime import timedelta
from botocore.exceptions import ClientError
s3 = boto3.resource("s3")
import path

basePath = path.func()

inputConfigurations = {
    'bucketName' : "redshift-mkart-s3",
    'inputDir' : basePath + 'input/',
    'outputDir' : basePath + 'output/',
    'monthlyWiseSalesDump' : basePath + 'monthwisesalesdump/',
    'bucketInputDirectory' : 'powerbi/input/masterfiles',
    'monthlySalesDataDirectory' : 'powerbi/monthwisesalesdump',
    'dumpDir' : path.func1(),
    'localDailyDumpDir' : basePath + 'dailydump/',
    'salesAbstractFileNamePattern' : 'salesabstractmonthly_' + datetime.date.today().strftime('%Y%m%d'),
    'salesDetailsFileNamePattern' : 'salesdetailmonthly_' + datetime.date.today().strftime('%Y%m%d'),
    'scoreCardFile' : basePath + 'scoreCard.py',
    'repeatCustomerFile' : basePath + 'repeatCustomer.py',
    'salesByTimeFile' : basePath + 'salesByTime.py',
    'repeatacutechronicFile' : basePath + 'repeatacutechronic.py',
}

def downloadAllMasterData(bucket_name, s3_folder, local_dir=None):
    """
    Download the contents of a folder directory
    Args:
        bucket_name: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
        local_dir: a relative or absolute directory path in the local file system
    """
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = obj.key if local_dir is None \
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)
        
def dowmloadTodaysDump():
    for file in os.listdir(inputConfigurations['dumpDir']):
        if file.startswith(inputConfigurations['salesAbstractFileNamePattern']) or file.startswith(inputConfigurations['salesDetailsFileNamePattern']):
            print(file)
            shutil.copy2(inputConfigurations['dumpDir'] + file,os.path.relpath(inputConfigurations['localDailyDumpDir']))
def extractAllZipFiles():
    dir_name = os.path.abspath(inputConfigurations['localDailyDumpDir'])
    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith('.zip'): # check for ".zip" extension
            file_name = os.path.join(dir_name,item) # get full path of files
            zip_ref = ZipFile(file_name) # create zipfile object
            zip_ref.extractall(dir_name) # extract file to dir
            zip_ref.close() # close file
            if(item.startswith(inputConfigurations['salesAbstractFileNamePattern'])):
                os.rename(file_name.replace('.zip', '.csv'),dir_name + '/salesAbstract.csv')
            if(item.startswith(inputConfigurations['salesDetailsFileNamePattern'])):
                os.rename(file_name.replace('.zip', '.csv'),dir_name + '/salesDetails.csv')
            os.remove(file_name)
            
def uploadOutputToAws():
    try:
        print("called")
        s3 = boto3.client('s3')
        folderPath = os.path.abspath(inputConfigurations['outputDir'])
        for file in os.listdir(folderPath):
            print(file)
            path = 'powerbi/reporting/sales/' + str((datetime.date.today()-timedelta(days=1)).year) + "/" + str((datetime.date.today()-timedelta(days=1)).strftime('%m')) + "/"
            s3 = boto3.client('s3')
            response = s3.upload_file(
            Filename = os.path.join(folderPath,file),
            Bucket = inputConfigurations['bucketName'],
            Key = path + file,
            ExtraArgs = {
                'ContentType':'text/csv',
                }
            ) 
    except ClientError as e:
        print(e)  
        
           
try:
    downloadAllMasterData(inputConfigurations['bucketName'],inputConfigurations['bucketInputDirectory'],inputConfigurations['inputDir'])
    dowmloadTodaysDump()
    downloadAllMasterData(inputConfigurations['bucketName'],inputConfigurations['monthlySalesDataDirectory'],inputConfigurations['monthlyWiseSalesDump'])
    extractAllZipFiles()
    
    #ScoreCard File exectution
    logger = logging.getLogger('my_logger')
    logging.basicConfig(filename=basePath+'logs/autogeneratePythonLogs.log', filemode='w', level=logging.DEBUG)
    os.system('python3 ' + inputConfigurations['scoreCardFile'] +" '"+ basePath+ "'")
    os.system('python3 ' + inputConfigurations['repeatCustomerFile'] +" '"+ basePath+ "'")
    os.system('python3 ' + inputConfigurations['salesByTimeFile'] +" '"+ basePath+ "'")
    os.system('python3 ' + inputConfigurations['repeatacutechronicFile'] +" '"+ basePath+ "'")
    
except Exception as e:
    logging.warning("--------------------------------------------------")
    logging.error("Oops! An exception has occured:" +  str(e))
    logging.error("Exception TYPE:" + str(type(e)))
    logging.warning("--------------------------------------------------")