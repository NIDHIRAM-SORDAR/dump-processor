import logging
import zipfile
import os
import pandas as pd
# create a logger object
logger = logging.getLogger(__name__)

# set the log level to INFO
logger.setLevel(logging.INFO)

# create a file handler object
handler = logging.FileHandler('mylog.log')

# create a formatter object
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# set the formatter for the handler
handler.setFormatter(formatter)

# add the handler to the logger
logger.addHandler(handler)

# log a message
logger.info('Starting my Python executable...')

# subsequent code goes here
# prompt user to enter a valid file path
for i in range(3):
    # get file path name from user input
    file_path = input("Enter file path name in the following format (e.g. C:\\Users\\username\\file.zip): ")

    # convert file path to compatible format
    file_path = file_path.replace("\\", "/")  # replace backslashes with forward slashes
    file_path = os.path.normpath(file_path)  # convert to compatible format

    # check if file exists in directory
    if os.path.isfile(file_path):
        print(f"File {file_path} exists.")
        break
    else:
        if i == 2:
            print("Maximum number of tries exceeded. Exiting program.")
            exit()
        else:
            print(f"File {file_path} does not exist. Please enter a valid file path. {2-i} attempts left.")
        
# subsequent code goes here


zip_path = file_path # path to your zip file

# get the directory where the zip file is located
zip_dir = os.path.dirname(zip_path)

# extract all files to the zip directory
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(zip_dir)
    
# get the path of the first extracted file
extracted_file_path = os.path.join(zip_dir, zip_ref.namelist()[0])
print("Extracted file path:", extracted_file_path)

df = pd.read_csv(extracted_file_path)

time_list = ['ALARM_TIME', 'CANCEL_TIME']

try:
    if all(elem in list(df.columns) for elem in time_list):
        print("Proceeding On")
    else:
        print("Data must include ALARM_TIME and CANCEL_TIME columns")
        exit()
except Exception as e:
    print("An error occurred while checking if List2 is a subset of List1:", e)

df['ALARM_TIME'] = pd.to_datetime(df['ALARM_TIME'], dayfirst=True)
df['CANCEL_TIME'] = pd.to_datetime(df['CANCEL_TIME'], dayfirst=True)
df['Down_time'] = df['CANCEL_TIME'] - df['ALARM_TIME']
# group the data by Category column and calculate the sum of Value column
sum_df = df.groupby('Name', as_index=False).agg({'Down_time': 'sum', 'TEXT': 'count'})
sum_df = sum_df.rename(columns={'Down_time': 'Sum_Down_time','TEXT':'Count_fluctuation'})
# calculate total seconds and convert to timedelta data type
sum_df['Sum_Down_time'] = pd.to_timedelta(sum_df['Sum_Down_time'].dt.total_seconds(), unit='s')

# format timedelta values to [h]:mm:ss
sum_df['Sum_Down_time'] = sum_df['Sum_Down_time'].apply(lambda x: str(int(x.total_seconds() // 3600)).zfill(2) + ':' + str(int(x.total_seconds() // 60 % 60)).zfill(2) + ':' + str(int(x.total_seconds() % 60)).zfill(2))
output_path = os.path.join(os.path.dirname(file_path), 'output.xlsx')
sum_df.to_excel(output_path, index=False)
print(f"Output written to {output_path}.")

logger.info('My Python executable completed.')
