### Date created
2/16/2021

### Project Title
INRIX_API Data Download, for single use and/or scheduled daily downloads

### Description
Python code for automated download of XD segment data from RITIS Roadway Analytics API, including speed and travel time. 

Required: Roadway Analytics account (request by emailing support@inrix.com)

Your Roadway Analytics email and password are used to request a security token, which is used to request the data. Requested data will download to user-specified folder. The user-specified folder mentioned must contain a text file named segments.txt which includes desired XD segment ID's separated by commas e.g. 1236865990,124548783,1236859215

The INXRIX_XD_Downloader module is used to initiate a single download for a specified date range. The INRIX_XD_Daily_Downloader can be used to call the aforementioned module on a schedule and download data for a single day at a time. It checks the date in the file last_run.txt and runs the downloader for every day from that date until yesterday, one day at a time. It also only saves the csv file, whereas the single downloader saves the entire folder with metadata in it. The purpose in having only csv files in a single folder is to ingest them into Power BI.

### Files Used
INXRIX_XD_Downloader.py  
INRIX_XD_Daily_Downloader.py

##### Files Not shown in repository:  
folder named Speed_Data, with the following inside it:  
folder named Daily_Download, where only csv files are stored  
daily_download_segments.txt, contains segment ID's to be downloaded each day  
last_run.txt, most recent day for which data was downloaded  
security_token.pickle, stores the security token. the module creates and manages this file  
segments.txt, contains segment ID's for the single downloader  