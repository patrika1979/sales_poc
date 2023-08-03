import pandas as pd
from openpyxl import load_workbook
from datetime import date
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import gcsfs
import re
import io
import numpy as np

def gcs_trigger_xlsx_loader(event, context):
    print(f"Print event {event}")
    filename = event
    bucket = event['bucket']

    #the filename has a pattern of sampledatahockey.*xlsx. Process the file only when the pattern macth
    pattern=re.compile(r'Source_path/base_[0-9]{4}.*xlsx$')
    if pattern.match(event['name']): #event['name'] contains the name part of the event, here the filename
        print('Reading file {file}'.format(file=event['name']))
        
        try:
            print('Process execution started')
            sales_file_load(bucket,filename)
            print('Process execution completed')
            
        except:
            print('Process failed')    
            exit(1)
    else :
        print('Error encountered. Filename does not match')


def sales_file_load(bucket, filename):
    src_bucket_name_param=str(bucket) #the bucket where the file is supposed to land
    
    project_name_param=bucket.replace('-data', '') #the GCP project name
    
    fs=gcsfs.GCSFileSystem(project=project_name_param)
    client=storage.Client()
    bucket=client.get_bucket(src_bucket_name_param)
    today=datetime.today().strftime("%Y%m%d_%H%M%S")

    #declare the filepaths
    input_file_path= filename['name']
    #input_file_path = absolute_path + filename['name']
    #output_file_path = f'Output_path/processed_{today}.csv'
    output_file_path = f'Output_path/processed.csv'
        
    #the tabs which we are supposed to read. 
    required_tabs=['Sheet1', 'Planilha1']
       
    #GCS cannot read a direct GCS filepath, so it must be declared as an instance of io.BytesIO class
    #io.BytesIO() is a class in Python's io module that creates an in-memory binary stream that can be used to read from or write to bytes-like objects. It provides an interface similar to that of a file object, allowing you to manipulate the contents of the stream using methods such as write(), read(), seek(), and tell()
    #One common use case for io.BytesIO() is when you want to work with binary data in memory instead of having to create a physical file on disk. 
    
    blob=bucket.blob(str(input_file_path))
    buffer=io.BytesIO()
    blob.download_to_file(buffer)
    
    #reading each tab in the list
    for sheet in required_tabs:
        try:
            #Simple pandas operation
            print('Started processing sheet {}'.format(sheet))
            df=pd.read_excel(buffer,sheet,header=[0])
            df.columns=[x.lower() for x in df.columns]
            df.insert(0,"extract_date", today, True)
            df.drop_duplicates(inplace=True)
            print('Completed processing sheet {}'.format(sheet))
        except:
            pass
        
    #This part highlights the process if you want the file as a CSV output 
    #logic starts here
    client=storage.Client()
    
    extract_datetime=str(datetime.today().strftime("%Y-%m-%d %H%M%S"))
    source_dir='Source_path/'
    output_dir='Output_path/'
    target_dir='Archive_path/archived_files_'+extract_datetime+'/'
    
   
    bucket.blob(output_file_path).upload_from_string(df.to_csv(index=False, encoding='utf-8'), 
content_type='application/octet-stream')
    print('Generated files for Sales data :{}'.format(output_file_path))
    