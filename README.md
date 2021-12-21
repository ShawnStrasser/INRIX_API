### Project Title
Travel Time Data Download and Analytics

Please send questions to shawn.strasser@odot.oregon.gov

### Description
Python code for automated download and analytics of XD segment data from INRIX Roadway Analytics API, including speed and travel time.

Required: Roadway Analytics account (request by emailing support@inrix.com)

Your Roadway Analytics email and password are used to request a security token, which is used to request the data. Requested data will download to user-specified folder. The user-specified folder mentioned must contain a text file named segments.txt which includes desired XD segment ID's separated by commas e.g. 1236865990,124548783,1236859215

The INXRIX_XD_Downloader module is used to initiate a single download for a specified date range. The INRIX_XD_Daily_Downloader can be used to call the aforementioned module on a schedule and download data for a single day at a time. It checks the date in the file last_run.txt and runs the downloader for every day from that date until yesterday, one day at a time. The INRIX_XD_Daily_Downloader saves the data in the parquet file format in a single folder. Then, those files can be loaded all at once to Power BI.

Additionally, there is a Jupiter notebook segments_generator which takes coordinates from traffic signals and joins them to XD segments, to create a dimension table which maps the two together, and to create a list of XD segments to be downloaded each day. Lastly, the analytics notebook performs a time series decomposition and other stats for each XD segment.

### Files Used
INXRIX_XD_Downloader.py  
INRIX_XD_Daily_Downloader.py
segments_generator.ipynb
analytics.ipynb

##### Files Not shown in repository:  
folder named Speed_Data, with the following inside it:  
folder named Daily_Download, where only csv files are stored  
daily_download_segments.txt, contains segment ID's to be downloaded each day  
last_run.txt, most recent day for which data was downloaded  
security_token.pickle, stores the security token. the module creates and manages this file  
segments.txt, contains segment ID's for the single downloader  