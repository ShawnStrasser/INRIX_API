## This downloads INRIX XD segemnt probe data through the INRIX Roadway Analytics API
## A Roadway Analyitics account must be requested first by emailing support@inrix.com
## Your Roadway Analytics email and password are used to request a security token, which is used to request the data
## Data is returned via a download link and/or automatically downloaded to a folder which you specify
## In the above folder, create a text file named segments.txt and include your XD segments separated by commas e.g. 1236865990,124548783,1236859215
## Disclaimer: use at your own risk, no assumptions of liability or otherwise and so forth etc. 

## Made with Python 3.6.5



#############################################################################
    ## 
    ## DEFINE VARIABLES & IMPORT MODULES
    ##
    #########################################################################

## USER DEFINED VARIABLES ##
email = 'shawn.strasser@odot.state.or.us'
folder = '//scdata2/signalshar/Data_Analysis/Python/Speed_Data/' #define the working folder where data will be saved
xd_segment_filename = 'segments.txt' # rename as needed, but must be a text file with segments separated by commas
start_date = '2020-10-01' # format must be 'yyyy-mm-dd'
end_date = '2020-10-30'   # end date is inclusive
bin_size = 15 # (1, 5, 15, or 60)
days_of_week = [ 1, 2, 3, 4, 5, 6, 7 ]
data_name = 'data' + start_date + '_' + end_date # name of the folder with the downloaded data
security_token_filename = 'security_token.pickle'
timezone = 'PST8PDT'
fields = [ "LOCAL_DATE_TIME", "XDSEGID", "SPEED", "TRAVEL_TIME", "CVALUE" ] # see documentation for available options
map_version = '2002'
seconds_to_sleep = 2

## IMPORT MODULES ##
import requests
import pickle 
import datetime
import time
import urllib.request
import os
import zipfile
import pandas as pd


## ROADWAY ANALYTICS API URL's ##
security_token_url = 'https://roadway-analytics-api.inrix.com/v1/auth'
data_download_url = 'https://roadway-analytics-api.inrix.com/v1/data-downloader'
report_status_url = 'https://roadway-analytics-api.inrix.com/v1/report/status/'
report_download_url = 'https://roadway-analytics-api.inrix.com/v1/data-downloader/'

## CALCULATED VARIABLES ##
now = datetime.datetime.utcnow()

with open(folder + xd_segment_filename, 'r') as file:
    xd_segments = file.read().split(',')

data_download_json = { "unit": "IMPERIAL", "fields": fields, "xdSegIds": xd_segments, "timezone": timezone, 
"dateRanges": [ { "start": start_date, "end": end_date, "daysOfWeek": days_of_week } ], "mapVersion": map_version, 
"reportType": "DATA_DOWNLOAD", "granularity": bin_size, #"emailAddresses": [ email ], #optionally, INRIX can send you an email when the download is ready
"includeClosures": "false" }


#############################################################################
    ##
    ## OBTAIN SECURITY TOKEN
    ##
    #########################################################################
# The security token is stored in a .pickle file, in the user specified folder, and named with the user specified name
# The token is stored so it can be reused for subsequent requests
# This is done because the documentation asks us not to request a new token until the previous token has expired
# If there is no security token file (first time running the code) OR the token has expired, a new one will be requested

# Open pickle file and read security token and it's expiration time
print('\n\n', '-' * 80, '\n Checking if security token is valid. Please wait.\n', '-' * 80, '\n\n')
try:
    with open(folder + security_token_filename, 'rb') as handle:
        existing_token = pickle.load(handle).json()
    security_token = existing_token["result"]["accessToken"]["token"]
    expiration = datetime.datetime.strptime(existing_token['result']["accessToken"]["expiry"], '%Y-%m-%dT%H:%M:%S.%fZ')
    file_not_exist = False
except:
    file_not_exist = True

# Request new token if it has or will expire within 5 minutes, or dosen't exist yet
if file_not_exist or now + datetime.timedelta(minutes=5) > expiration:
    password = input('\n\nSecurity token not found or expired.\nPlease enter your Roadway Analytics password to request new token: ') 
    security_token_json = {"email" : email,"password" : password}
    r = requests.post(security_token_url, json = security_token_json)
    security_token = r.json()["result"]["accessToken"]["token"]
    with open(folder + security_token_filename, 'wb') as handle:
        pickle.dump(r, handle, protocol=pickle.HIGHEST_PROTOCOL)
# Now an active security token is stored in the security_token variable


#############################################################################
    ##
    ## POST THE REQUEST AND WAIT UNTIL REPORT IS READY
    ##
    #########################################################################
# This step uses the security token and user inputs to request historical XD segment data
r = requests.post(data_download_url, json = data_download_json, headers={"Authorization": "Bearer " + security_token})

# The report id returned from the above request is needed to check the status of the request
report_id = r.json()["reportId"]

# Check report status
job_status_state = 'IN_PROGRESS'
while job_status_state == 'IN_PROGRESS':
    job_status = requests.get(report_status_url + report_id, headers={"Authorization": "Bearer " + security_token})
    job_status_state = job_status.json()["state"]
    print(job_status.json()["progress"])
    time.sleep(seconds_to_sleep)
 
# If we've made it through the while loop sucessuflly, that should mean the report is ready! :)
print('\n\n', '-' * 80, '\n REQUEST SUCCESS!!!\n\n Please wait while data is downloaded and organized.\n', '-' * 80, '\n\n')


#############################################################################
    ##
    ## DOWNLOAD AND EXTRACT REPORT
    ##
    #########################################################################
# Get the download link for the report
r = requests.get(report_download_url + report_id, headers={"Authorization": "Bearer " + security_token})
download_link = r.json()["urls"]

# Convert link to a string and lose the brackets and quotation marks...this right here was a pain to figure out!
download_link = str(download_link)[2:-2]

# Download the file into a zip folder, using the report_id as the temporary name
urllib.request.urlretrieve(download_link, folder + report_id +'.zip')

# Extract contents of the zipped folder
with zipfile.ZipFile(folder + report_id + '.zip', 'r') as zip_ref:
    zip_ref.extractall(folder)

# Delete the zipped folder
os.remove(folder + report_id + '.zip')

# Rename the folder, and "_copy" to the end if the name is taken
conflict_check = ''
emergency_exit = 0
while emergency_exit <= 10:
    try:
        # the name of the folder inside the zipped folder is the report id with "_part_1" on the end
        os.rename(folder + report_id + '_part_1', folder + data_name + conflict_check)
        break
    except:
        conflict_check += '_copy'
        emergency_exit += 1


#############################################################################
    ##
    ## CONVERT TIMESTAMP TO DATETIME
    ##
    #########################################################################
print('\nConverting Date Time column to timestamp format which can be read by Excel, this could take a while, please wait.\n')
# Read csv file into a dataframe
df = pd.read_csv(folder + data_name + conflict_check + '/data.csv')
# Convert Date Time column to datetime formate
df['Date Time'] = pd.to_datetime(df['Date Time'])
# Remove timezone
df['Date Time'] = df['Date Time'].dt.tz_localize(None)
# Write back to the file
df.to_csv(folder + data_name + conflict_check + '/data.csv', index=False)

print('\n\n', '-' * 80, '\n DOWNLOAD SUCCESS!\n Everything is now complete, you may access the data in ' + folder + '\n', '-' * 80, '\n\n')

