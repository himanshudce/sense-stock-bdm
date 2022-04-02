
import pandas as pd
from nsetools import Nse
import os





base_location = 'data_sources/stock_data'


try:
    os.makedirs(base_location)
except:
    pass

date = pd.Timestamp.now(tz='Asia/Kolkata')
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)

base_location_date= os.path.join(base_location,date_str)
print(base_location_date)

nse = Nse() #stock API

try:
    os.makedirs(base_location_date)
except:
    pass


companies = pd.read_csv('top_25NSE.csv')
companies_list = list(companies['Company Name'].values)

def pull_stock_data(nse,companies_list):
    stock =pd.DataFrame()
    
    for i in companies_list:

        q= nse.get_quote(i)
        if q is None:
            continue
        else:
            stock = stock.append(pd.DataFrame(q,index=[0]),ignore_index=True)
    return stock






    

date = pd.Timestamp.now(tz='Asia/Kolkata')
hour = str(date.hour)
min = str(date.minute)
csv_name = 'stock_data_'+hour+'_'+min+'.csv'
csv_location = os.path.join(base_location_date,csv_name)
print("\n saving data location is ",csv_location)

# converting data to csv
stock_data = pull_stock_data(nse,companies_list)
stock_data_csv = stock_data.to_csv()
stock_data.to_csv(csv_location)

print("data saved to disk \n")
    
