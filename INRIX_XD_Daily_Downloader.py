## This downloads daily INRIX XD segemnt data, it is intened to be run using the Windows Task Scheduler
## Disclaimer: use at your own risk, no assumptions of liability or otherwise and so forth etc. 


import INRIX_XD_Downloader as d
import datetime

last_run_file = d.folder + 'last_run.txt'
d.xd_segment_filename = 'daily_download_segments.txt' 
today = datetime.datetime.now().date()

with open(last_run_file, 'r') as file:
    last_run = file.read()

last_run = datetime.datetime.strptime(last_run, '%Y-%m-%d').date()
print('good')
while last_run < today - datetime.timedelta(days=1):
    d.start_date = str(last_run + datetime.timedelta(days=1))
    d.end_date = d.start_date
    print('\nWORKING ON: ' + d.start_date)
    d.main()
    with open(last_run_file, 'w') as file:
        file.write(d.start_date)
    last_run += datetime.timedelta(days=1)

print('=' * 100 + '\nDaily download complete\n' + '=' * 100)

print('Running analytics now')
exec(open("analytics.py").read())
print('done')