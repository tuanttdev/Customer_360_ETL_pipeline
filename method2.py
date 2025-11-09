import os
from etl_script import *
import datetime

start_time = datetime.datetime.now()

path = './log_content'
save_path = './clean_data'

print('--------------Read all file then process in one spark dataframe------------')
files_path = [path + '/' + file for file in os.listdir(path)]
save_path = save_path + '/total_method2'
main(files_path, save_path)

endtime = datetime.datetime.now() ## 93s , 469s , 575s
timedelta = endtime - start_time
print(timedelta.total_seconds())