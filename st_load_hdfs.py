
import pandas as pd
# from nsetools import Nse
import os
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
from hdfs.ext.avro import AvroReader
import glob
import re
import json
import shutil

web_hdfs_interface = InsecureClient("http://localhost:9870", user="bdm")
#web_hdfs_interface.list("data_sources/stock_data")

#nse = Nse()
base_location = "data_sources/stock_data"
web_hdfs_interface.makedirs(base_location)

date = pd.Timestamp.now(tz="Asia/Kolkata")
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)
local_location_date = os.path.join(base_location,date_str)



  
# loop over the list of csv files
def all_data_today(csv_files):
    wdf = pd.DataFrame()
    for f in csv_files:
        print(f) 
    
  
        #find date and time
        m = re.findall(r"\d+",f)
        # read the csv file
        df = pd.read_csv(f)
        #append timestamp in dataframe at index 0
        df.insert(0, "Timestamp", "_".join(m))
    
        # append dataframe 
        wdf = wdf.append(df,ignore_index=True)
    wdf.drop("Unnamed: 0", axis=1, inplace=True)
    wdf = wdf.applymap(str)
    return wdf

def store_in_avro(avro_location,web_hdfs_interface,parsed,schem):
    with AvroWriter(web_hdfs_interface, avro_location,schema = schem) as writer:
        for record in parsed:
            writer.write(record)
schem = {
    "type" : "record",
    "name" : "stock",
    "namespace" : "one",
    "fields" : [{"name": "index", "type": "string","default" : "None"}, {"name": "Unnamed: 0", "type": "string","default" : "None"}, {"name": "pricebandupper", "type": "string","default" : "None"}, {"name": "symbol", "type": "string","default" : "NONE"}, {"name": "applicableMargin", "type": "string","default" : "None"}, {"name": "bcEndDate", "type": "string","default" : "NONE"}, {"name": "totalSellQuantity", "type": "string","default" : "None"}, {"name": "adhocMargin", "type": "string","default" : "None"}, {"name": "companyName", "type": "string","default" : "NONE"}, {"name": "marketType", "type": "string","default" : "NONE"}, {"name": "exDate", "type": "string","default" : "NONE"}, {"name": "bcStartDate", "type": "string","default" : "NONE"}, {"name": "css_status_desc", "type": "string","default" : "NONE"}, {"name": "dayHigh", "type": "string","default" : "None"}, {"name": "basePrice", "type": "string","default" : "None"}, {"name": "securityVar", "type": "string","default" : "None"}, {"name": "pricebandlower", "type": "string","default" : "None"}, {"name": "sellQuantity5", "type": "string","default" : "None"}, {"name": "sellQuantity4", "type": "string","default" : "None"}, {"name": "sellQuantity3", "type": "string","default" : "None"}, {"name": "cm_adj_high_dt", "type": "string","default" : "NONE"}, {"name": "sellQuantity2", "type": "string","default" : "None"}, {"name": "dayLow", "type": "string","default" : "None"}, {"name": "sellQuantity1", "type": "string","default" : "None"}, {"name": "quantityTraded", "type": "string","default" : "None"}, {"name": "pChange", "type": "string","default" : "None"}, {"name": "totalTradedValue", "type": "string","default" : "None"}, {"name": "deliveryToTradedQuantity", "type": "string","default" : "None"}, {"name": "totalBuyQuantity", "type": "string","default" : "None"}, {"name": "averagePrice", "type": "string","default" : "None"}, {"name": "indexVar", "type": "string","default" : "None"}, {"name": "cm_ffm", "type": "string","default" : "None"}, {"name": "purpose", "type": "string","default" : "NONE"}, {"name": "buyPrice2", "type": "string","default" : "None"}, {"name": "secDate", "type": "string","default" : "NONE"}, {"name": "buyPrice1", "type": "string","default" : "None"}, {"name": "high52", "type": "string","default" : "None"}, {"name": "previousClose", "type": "string","default" : "None"}, {"name": "ndEndDate", "type": "string","default" : "None"}, {"name": "low52", "type": "string","default" : "None"}, {"name": "buyPrice4", "type": "string","default" : "None"}, {"name": "buyPrice3", "type": "string","default" : "None"}, {"name": "recordDate", "type": "string","default" : "NONE"}, {"name": "deliveryQuantity", "type": "string","default" : "None"}, {"name": "buyPrice5", "type": "string","default" : "None"}, {"name": "priceBand", "type": "string","default" : "NONE"}, {"name": "extremeLossMargin", "type": "string","default" : "None"}, {"name": "cm_adj_low_dt", "type": "string","default" : "NONE"}, {"name": "varMargin", "type": "string","default" : "None"}, {"name": "sellPrice1", "type": "string","default" : "None"}, {"name": "sellPrice2", "type": "string","default" : "None"}, {"name": "totalTradedVolume", "type": "string","default" : "None"}, {"name": "sellPrice3", "type": "string","default" : "None"}, {"name": "sellPrice4", "type": "string","default" : "None"}, {"name": "sellPrice5", "type": "string","default" : "None"}, {"name": "change", "type": "string","default" : "None"}, {"name": "surv_indicator", "type": "string","default" : "None"}, {"name": "ndStartDate", "type": "string","default" : "None"}, {"name": "buyQuantity4", "type": "string","default" : "None"}, {"name": "isExDateFlag", "type": "string","default" : "None"}, {"name": "buyQuantity3", "type": "string","default" : "None"}, {"name": "buyQuantity2", "type": "string","default" : "None"}, {"name": "buyQuantity1", "type": "string","default" : "None"}, {"name": "series", "type": "string","default" : "NONE"}, {"name": "faceValue", "type": "string","default" : "None"}, {"name": "buyQuantity5", "type": "string","default" : "None"}, {"name": "closePrice", "type": "string","default" : "None"}, {"name": "open", "type": "string","default" : "None"}, {"name": "isinCode", "type": "string","default" : "NONE"}, {"name": "lastPrice", "type": "string","default" : "None"}]
} 
companies = pd.read_csv("top_25NSE.csv")
companies_list = list(companies["Company Name"].values)

# extract files from local system of today

csv_files = glob.glob(os.path.join(local_location_date, "*.csv"))

wdf = all_data_today(csv_files)  

avro_file = date_str+".avro"  #avro file name

#schema of our files 

schem = {
    "type" : "record",
    "name" : "stock",
    "namespace" : "one",
    "fields" : [{"name": "Timestamp", "type": "string","default" : "None"}, {"name": "pricebandupper", "type": "string","default" : "None"}, {"name": "symbol", "type": "string","default" : "NONE"}, {"name": "applicableMargin", "type": "string","default" : "None"}, {"name": "bcEndDate", "type": "string","default" : "NONE"}, {"name": "totalSellQuantity", "type": "string","default" : "None"}, {"name": "adhocMargin", "type": "string","default" : "None"}, {"name": "companyName", "type": "string","default" : "NONE"}, {"name": "marketType", "type": "string","default" : "NONE"}, {"name": "exDate", "type": "string","default" : "NONE"}, {"name": "bcStartDate", "type": "string","default" : "NONE"}, {"name": "css_status_desc", "type": "string","default" : "NONE"}, {"name": "dayHigh", "type": "string","default" : "None"}, {"name": "basePrice", "type": "string","default" : "None"}, {"name": "securityVar", "type": "string","default" : "None"}, {"name": "pricebandlower", "type": "string","default" : "None"}, {"name": "sellQuantity5", "type": "string","default" : "None"}, {"name": "sellQuantity4", "type": "string","default" : "None"}, {"name": "sellQuantity3", "type": "string","default" : "None"}, {"name": "cm_adj_high_dt", "type": "string","default" : "NONE"}, {"name": "sellQuantity2", "type": "string","default" : "None"}, {"name": "dayLow", "type": "string","default" : "None"}, {"name": "sellQuantity1", "type": "string","default" : "None"}, {"name": "quantityTraded", "type": "string","default" : "None"}, {"name": "pChange", "type": "string","default" : "None"}, {"name": "totalTradedValue", "type": "string","default" : "None"}, {"name": "deliveryToTradedQuantity", "type": "string","default" : "None"}, {"name": "totalBuyQuantity", "type": "string","default" : "None"}, {"name": "averagePrice", "type": "string","default" : "None"}, {"name": "indexVar", "type": "string","default" : "None"}, {"name": "cm_ffm", "type": "string","default" : "None"}, {"name": "purpose", "type": "string","default" : "NONE"}, {"name": "buyPrice2", "type": "string","default" : "None"}, {"name": "secDate", "type": "string","default" : "NONE"}, {"name": "buyPrice1", "type": "string","default" : "None"}, {"name": "high52", "type": "string","default" : "None"}, {"name": "previousClose", "type": "string","default" : "None"}, {"name": "ndEndDate", "type": "string","default" : "None"}, {"name": "low52", "type": "string","default" : "None"}, {"name": "buyPrice4", "type": "string","default" : "None"}, {"name": "buyPrice3", "type": "string","default" : "None"}, {"name": "recordDate", "type": "string","default" : "NONE"}, {"name": "deliveryQuantity", "type": "string","default" : "None"}, {"name": "buyPrice5", "type": "string","default" : "None"}, {"name": "priceBand", "type": "string","default" : "NONE"}, {"name": "extremeLossMargin", "type": "string","default" : "None"}, {"name": "cm_adj_low_dt", "type": "string","default" : "NONE"}, {"name": "varMargin", "type": "string","default" : "None"}, {"name": "sellPrice1", "type": "string","default" : "None"}, {"name": "sellPrice2", "type": "string","default" : "None"}, {"name": "totalTradedVolume", "type": "string","default" : "None"}, {"name": "sellPrice3", "type": "string","default" : "None"}, {"name": "sellPrice4", "type": "string","default" : "None"}, {"name": "sellPrice5", "type": "string","default" : "None"}, {"name": "change", "type": "string","default" : "None"}, {"name": "surv_indicator", "type": "string","default" : "None"}, {"name": "ndStartDate", "type": "string","default" : "None"}, {"name": "buyQuantity4", "type": "string","default" : "None"}, {"name": "isExDateFlag", "type": "string","default" : "None"}, {"name": "buyQuantity3", "type": "string","default" : "None"}, {"name": "buyQuantity2", "type": "string","default" : "None"}, {"name": "buyQuantity1", "type": "string","default" : "None"}, {"name": "series", "type": "string","default" : "NONE"}, {"name": "faceValue", "type": "string","default" : "None"}, {"name": "buyQuantity5", "type": "string","default" : "None"}, {"name": "closePrice", "type": "string","default" : "None"}, {"name": "open", "type": "string","default" : "None"}, {"name": "isinCode", "type": "string","default" : "NONE"}, {"name": "lastPrice", "type": "string","default" : "None"}]
} 
#wdf.columns
for c_symbol in companies_list: 
    
    #seperate data of each company  
    c_data = wdf[wdf["symbol"] == c_symbol]
    c_data.columns
    # if stock api couln"t get some companies data skip it
    if c_data.empty:
        continue
    
    #path of each company in hdfs
    hdfs_company_path = os.path.join(base_location,c_symbol)
    
    # create hdfs dir if it"s not already created, it doen"t overwrite if directory is already created
    web_hdfs_interface.makedirs(hdfs_company_path)
    
    #inside each company"s directory, we are storing every day"s data in one avro file
    avro_location = os.path.join(hdfs_company_path,avro_file)
    print("\n saving data location is ",avro_location)
    result = c_data.to_json(orient="records")
    parsed = json.loads(result)
    store_in_avro(avro_location,web_hdfs_interface,parsed,schem)
    print("\n saved")
        
    

#remove from local system after loading it to hdfs
if os.path.exists(local_location_date):
    shutil.rmtree(local_location_date)

   
        
   




#avro_name = "stock_data_"+hour+"_"+min+".avro"

    
#stock_data_csv = stock_data.to_csv()
#web_hdfs_interface.write(json_location,stock_data_csv)
#print("data saved to disk \n")

# to read the data
# with AvroReader(web_hdfs_interface, "data_sources/stock_data/BPCL/2022_04_01.avro") as reader:
#     schema = reader.schema # The remote file"s Avro schema.
#     print("schema",schema)
#     content = reader.content # Content metadata (e.g. size).
#     print("content",content)
#     for record in reader:
#         print("record",record) # and its records


    

# hdfs fsck /user/bdm/data_sources/stock_data/BPCL/2022_04_02.avro -files -blocks -locations
#web_hdfs_interface.list("data_sources/stock_data/BPCL")

# web_hdfs_interface.list(hdfs_location_date)[:]

#web_hdfs_interface.delete("data_sources/stock_data/BPCL/",recursive=True)