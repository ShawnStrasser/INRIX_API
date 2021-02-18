## Initiates download for INRIX XD segemnt data through the Roadway Analytics API, to your local folder
## Run this module for a single download at a time for the date range specified
## Create a text file named segments.txt and include your XD segments separated by commas e.g. 1236865990,124548783,1236859215
## Disclaimer: use at your own risk, no assumptions of liability or otherwise and so forth etc. 

## Made with Python 3.6.5

#############################################################################
    ## 
    ## DEFINE VARIABLES & IMPORT MODULES
    ##
    #########################################################################

## USER DEFINED VARIABLES ##
folder = '//scdata2/signalshar/Data_Analysis/INRIX_API/Speed_Data/' #define the working folder where data will be saved, make sure your segments.txt file is there
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
seconds_to_sleep = 5

# daily_folder is where daily downloads will be stored. it is not used in this module, it will only be used by the Daily_Downloader module
daily_folder = 'Daily_Download/' 

## ROADWAY ANALYTICS API URL's ##
security_token_url = 'https://roadway-analytics-api.inrix.com/v1/auth'
data_download_url = 'https://roadway-analytics-api.inrix.com/v1/data-downloader'
report_status_url = 'https://roadway-analytics-api.inrix.com/v1/report/status/'
report_download_url = 'https://roadway-analytics-api.inrix.com/v1/data-downloader/'

## IMPORT MODULES ##
import requests
import pickle 
import datetime
import time
import urllib.request
import os
import shutil
import zipfile
import pandas as pd
import keyring

## CREDENTIALS ##
def get_credentials():
    try:
        email = keyring.get_password('INRIX_Roadway_Analytics', 'email')
        password = keyring.get_password('INRIX_Roadway_Analytics', email)
        assert(email != None)
        assert(password != None)
    except:
        email = input('\n\n' + '-' * 80 + '\n\nPlease enter the email and password for your INRIX Roadway Analytics account.\nEmail: ')
        password = input('Password: ')
        save = input("\nStore email/password locally for later? You can manage them later using the Windows Credential Manager.\nStore email/password in Credential Manger? Type YES or NO: ")
        if save.lower() == 'yes':
            keyring.set_password('INRIX_Roadway_Analytics','email', email)
            keyring.set_password('INRIX_Roadway_Analytics', email, password)
            print('\n\nYour email and password are now stored in the Credential Manager under the name INRIX_Roadway_Analytics.')
            print('There are two credentials with that name, one used to look up your email, and the other uses your email to look up your password.')
    return email, password



## CALCULATED VARIABLES ##
now = datetime.datetime.utcnow()

def create_json():
    with open(folder + xd_segment_filename, 'r') as file:
        xd_segments = file.read().split(',')

    data_download_json = { "unit": "IMPERIAL", "fields": fields, "xdSegIds": xd_segments, "timezone": timezone, 
    "dateRanges": [ { "start": start_date, "end": end_date, "daysOfWeek": days_of_week } ], "mapVersion": map_version, 
    "reportType": "DATA_DOWNLOAD", "granularity": bin_size, #"emailAddresses": [ email ], #optionally, INRIX can send you an email when the download is ready
    "includeClosures": "false" }
    return data_download_json


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
def request_token(email, password):
    print('\nChecking if security token is valid. Please wait.')
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
        print('Requesting new security token.')
        security_token_json = {"email" : email,"password" : password}
        r = requests.post(security_token_url, json = security_token_json)
        security_token = r.json()["result"]["accessToken"]["token"]
        with open(folder + security_token_filename, 'wb') as handle:
            pickle.dump(r, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return security_token
# Now an active security token is stored in the security_token variable


#############################################################################
    ##
    ## POST THE REQUEST AND WAIT UNTIL REPORT IS READY
    ##
    #########################################################################
# This step uses the security token and user inputs to request historical XD segment data
def request_data(data_download_json, security_token):
    r = requests.post(data_download_url, json = data_download_json, headers={"Authorization": "Bearer " + security_token})
# The report id returned from the above request is needed to check the status of the request
    report_id = r.json()["reportId"]
# Check report status
    job_status_state = 'IN_PROGRESS'
    job_progress = '0%' + ' completed'
    while job_status_state == 'IN_PROGRESS':
        time.sleep(seconds_to_sleep)
        job_status = requests.get(report_status_url + report_id, headers={"Authorization": "Bearer " + security_token})
        job_status_state = job_status.json()["state"]
        if job_progress != job_status.json()["progress"]:
            job_progress = job_status.json()["progress"]
            print('Job Status on the RITIS side: ' + job_progress)
    # If we've made it through the while loop sucessuflly, that should mean the report is ready! :)
    print('REQUEST SUCCESS!!! Please wait while data is downloaded and organized.')
    return report_id

#############################################################################
    ##
    ## DOWNLOAD AND EXTRACT REPORT
    ##
    #########################################################################
# Get the download link for the report
def download_and_extract(report_id, security_token):
    r = requests.get(report_download_url + report_id, headers={"Authorization": "Bearer " + security_token})
    download_link = r.json()["urls"]
    # Convert link to a string and lose the brackets and quotation marks
    download_link = str(download_link)[2:-2]
    # Download the file into a zip folder, using the report_id as the temporary name
    urllib.request.urlretrieve(download_link, folder + report_id +'.zip')
    # Extract contents of the zipped folder
    with zipfile.ZipFile(folder + report_id + '.zip', 'r') as zip_ref:
        zip_ref.extractall(folder)
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
    # Delete the zipped folder
    os.remove(folder + report_id + '.zip')
    print('Converting Date Time column to timestamp format which can be read by Excel, this could take a while, please wait.')
    # Read csv file into a dataframe
    df = pd.read_csv(folder + data_name + conflict_check + '/data.csv')
    # Convert Date Time column to datetime formate
    df['Date Time'] = pd.to_datetime(df['Date Time'])
    # Remove timezone
    df['Date Time'] = df['Date Time'].dt.tz_localize(None)
    # Write back to the file
    df.to_csv(folder + data_name + conflict_check + '/data.csv', index=False)
    return conflict_check

# This function will only be called from the Daily Downloader module
def for_daily_downloader(conflict_check):
    shutil.move(folder + data_name + conflict_check + '/data.csv', folder + daily_folder + start_date + '.csv')
    time.sleep(seconds_to_sleep)
    shutil.rmtree(folder + data_name + conflict_check)

# One function to rule them all...
def main():
    email, password = get_credentials()
    data_download_json = create_json()
    security_token = request_token(email, password)
    report_id = request_data(data_download_json, security_token)
    conflict_check = download_and_extract(report_id, security_token)
    if __name__ == '__main__':
        print('\n\n', '-' * 100, '\n DOWNLOAD SUCCESS!\n Everything is now complete, you may access the data in ' + folder + '\n', '-' * 100, '\n\n')
    else:
        for_daily_downloader(conflict_check)

# Only call the main function if this module is being run, as opposed to if it is imported into the Daily Downloader module
if __name__ == '__main__':
    main()
    
