import os

from etl_script import main
import datetime

start_time = datetime.datetime.now()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(BASE_DIR, 'log_content')

print('--------------Run each file seperately--------------')
for file in os.listdir(path):

    file_path = path + '/' + file
    file_name = os.path.splitext(file)[0]
    print(f'file name : {file_path}')
    save_path = './clean_data' + '/' + file_name
    main(file_path, save_path)

endtime = datetime.datetime.now()
timedelta = endtime - start_time
print(timedelta.total_seconds()) ## 116s, 614s , 671s