# Big Data Pipeline (Sense Stock)
The project aims to build a big data pipeline for our business idea Stock sense where we Aimed to provide stock data along with social media data in order to help in the invetment providing better sense to investment. The detailed Idea can be seen here - 
[Stock Sense Slides](https://drive.google.com/file/d/1pVkBId5rmK1ff_Kw644yA6481FHEf_pG/view?usp=sharing)

To install the requirments run
```
pip install -r requirments.txt
```

**The Data Pipeline :**

![img](src/Pipeline.png)

**---------------------------------------------------------------------------------------------------------------------------------**

Three Phases Development :
<br><br>

# Phase 1
The high level view of Phase one data pipeline is indicated below - 
![img](src/high_level_view.png)

<br>

## 1. Setup Data Sources
**1. Twitter API** 

Setup The twitter developer account to access the API keys, It Can be done from [create deverloper account](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api)

Detailed Video - [How to setup Tweet](https://www.youtube.com/watch?v=Lu1nskBkPJU)
<br/>

**2. Stock API**

clone the repo
```
https://github.com/vsjha18/nsetools
```
nsetools - provides the real-time stock market data for National Stock Exchange, India.
Further details can be found here - [nsedocs](https://nsetools.readthedocs.io/en/latest/) 

<br />

**3. Companies List**

We used the top 25 companies listed here in [NSE Top 100](https://www.moneycontrol.com/stocks/marketinfo/marketcap/nse/index.html)
We have provided the csv file with top 25 companies listed for NSE.

**Any granulrities can be adapted when scalling the project**

<br><br>

## 2. Setup Data Collectors (DataBases/File Systems)

We are using the Database presented in Virtual Machine provided by UPC on cloud Systems. So the Databases are already setted up in VM. We are using the following Databases in our pipeline.

## A. HDFS 

To start server on VM use
```
/home/bdm/BDM_Software/hadoop/sbin/start-dfs.sh
``` 

## B. MongoDB 

a. modify the libcurl package using the below commands
```
sudo apt-get remove libcurl4
sudo apt-get install libcurl3
```
b. Open tmux session in detached mode and keep running mongoDB server on backend
```
tmux new -s mongodb
BDM_Software/mongodb/bin/mongod --bind_ip_all --dbpath /home/bdm/BDM_Software/data/mongodb_data/
```
more on tmux session - [Tmux](https://tmuxcheatsheet.com/)

<br><br>

## 3. Running Instructions 
**1. clone the repo**
```
git clone https://github.com/himanshudce/sense-stock-bdm
```
<br>

**2. run the below files to get the data locally**

a. For tweets (place your API keys)
```
python3 tw_load_local.py
```
** for ease, I have provided my API keys for tweets
<br>

b. For Stock data
```
python3 st_load_local.py
```

**After loading it will give the following file structure**

![img](src/temporal_land.png)

**3. Load data in mongo and hdfs**

a. To load tweet data in mongo
```
python3 tw_load_mongo.py
```

b. To load stock data in hdfs
```
python3 st_load_hdfs.py
```


## Note

The above files run automatically for each day when stock market gets open from 9 to 4 IST(Indian Standart time, tz = Asia/Kolkata) 

We run these files in VM using crontab in unix environment, which execute the file every day accordint to pattern. [Crontab](https://crontab.guru/)

Below are the instruction to setup. Simply type 
```
crontab -e 
```
to edit the file in any mode (nano,vim,etc)

```
0 4-11 * * * python3 /home/bdm/tw_load_local.py >> /home/bdm/logs/tw_logs.log;
0 12 * * * python3 /home/bdm/tw_load_mongo.py >> /home/bdm/logs/tw_mongo_logs.log;
*/1 4-11 * * * python3 /home/bdm/st_load_local.py >> /home/bdm/logs/st_local_logs.log;
0 12 * * * python3 /home/bdm/st_load_hdfs.py >> /home/bdm/logs/st_hdfs_logs.log
```



