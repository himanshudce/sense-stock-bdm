
import pandas as pd
# from nsetools import Nse
import os
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
import glob
import re

web_hdfs_interface = InsecureClient('http://localhost:9870', user='bdm')
#web_hdfs_interface.list('data_sources/stock_data')

#nse = Nse()
base_location = 'data_sources/stock_data'
web_hdfs_interface.makedirs(base_location)

date = pd.Timestamp.now(tz='Asia/Kolkata')
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)
local_location_date = os.path.join(base_location,date_str)



  
# loop over the list of csv files
def all_data_today(csv_files):
    wdf = pd.DataFrame()
    for f in csv_files:
        print(f) 
    
  
        #find date and time
        m = re.findall(r'\d+',f)
        # read the csv file
        df = pd.read_csv(f)
        #append timestamp in dataframe at index 0
        df.insert(0, 'Timestamp', '_'.join(m))
    
        # append dataframe 
        wdf = wdf.append(df,ignore_index=True)
    wdf.drop("Unnamed: 0", axis=1, inplace=True)
    return wdf

def store_in_avro(avro_location,web_hdfs_interface,c_data):
    with AvroWriter(web_hdfs_interface, avro_location) as writer:
        for record in c_data:
            writer.write(record)

companies = pd.read_csv('top_25NSE.csv')
companies_list = list(companies['Company Name'].values)

# extract files from local system of today

csv_files = glob.glob(os.path.join(local_location_date, "*.csv"))

wdf = all_data_today(csv_files)  

avro_file = date_str+'.avro'  #avro file name


for c_symbol in companies_list: 
    
    #seperate data of each company  
    c_data = wdf[wdf["symbol"] == c_symbol]
    
    # if stock api couln't get some companies data skip it
    if c_data.empty:
        continue
    
    #path of each company in hdfs
    hdfs_company_path = os.path.join(base_location,c_symbol)
    
    # create hdfs dir if it's not already created, it doen't overwrite if directory is already created
    web_hdfs_interface.makedirs(hdfs_company_path)
    
    #inside each company's directory, we are storing every day's data in one avro file
    avro_location = os.path.join(hdfs_company_path,avro_file)
    print("\n saving data location is ",avro_location)

    store_in_avro(avro_location,web_hdfs_interface,c_data)
    print("\n saved")
        
    

            




#avro_name = 'stock_data_'+hour+'_'+min+'.avro'

    
#stock_data_csv = stock_data.to_csv()
#web_hdfs_interface.write(json_location,stock_data_csv)
#print("data saved to disk \n")

# to list the data

web_hdfs_interface.list('data_sources/stock_data')
# web_hdfs_interface.list(hdfs_location_date)[:]
# web_hdfs_interface.delete('data_sources/stock_data',recursive=True)