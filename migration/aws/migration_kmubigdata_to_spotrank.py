import os
import boto3
import tsquery
import tsupload
import pandas as pd
from multiprocessing import Pool

import time
import pytz
from datetime import datetime, timedelta


SAVE_FILENAME = 'latest.csv.gz'
QUERY_PROFILE_NAME = 'kmubigdata'
UPLOAD_PROFILE_NAME = 'spotrank'
BUCKET_NAME = 'spotlake-test'
REGION_NAME = "us-west-2"
QUERY_DATABASE_NAME = "spotrank-timestream"
QUERY_TABLE_NAME = "spot-table"
UPLOAD_DATABASE_NAME = 'spotlake'
UPLOAD_TABLE_NAME = 'aws'
NUM_CPUS = 8
if 24 % NUM_CPUS != 0:
    raise Exception('use only 1, 2, 3, 4, 6, 8, 12, 24')
CHUNK_HOUR = 24 / NUM_CPUS

start_date = datetime(2022, 9, 28, 16, 20, 0, 0, pytz.UTC)
end_date = datetime(2022, 9, 29, 3, 10, 0, 0, pytz.UTC)

tsquery.PROFILE_NAME = QUERY_PROFILE_NAME # tsquery.PROFILE_NAME must be credential of source database
tsquery.REGION_NAME = REGION_NAME
tsquery.DATABASE_NAME = QUERY_DATABASE_NAME
tsquery.TABLE_NAME = QUERY_TABLE_NAME
tsupload.PROFILE_NAME = UPLOAD_PROFILE_NAME
tsupload.REGION_NAME = REGION_NAME
tsupload.DATABASE_NAME = UPLOAD_DATABASE_NAME
tsupload.TABLE_NAME = UPLOAD_TABLE_NAME

workload_cols = ['InstanceType', 'Region', 'AZ']
feature_cols = ['SPS', 'IF', 'SpotPrice']


# compress data as gzip file, save to local file system, upload file to s3
def save_gz_s3(df, timestamp):
    # compress and save to LFS
    df.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    
    # upload compressed file to S3
    session = boto3.Session(profile_name=UPLOAD_PROFILE_NAME)
    s3 = session.client('s3')
    s3_dir_name = '/'.join(timestamp.split()[0].split('-'))
    s3_obj_name = timestamp.split()[1]
    
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")


def compare_nparray(previous_df, current_df, workload_cols, feature_cols):  
    previous_df.loc[:,'Workload'] = previous_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    previous_df.loc[:,'Feature'] = previous_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:,'Workload'] = current_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:,'Feature'] = current_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)

    current_indices = current_df[['Workload', 'Feature']].sort_values(by='Workload').index
    current_values = current_df[['Workload', 'Feature']].sort_values(by='Workload').values
    previous_indices = previous_df[['Workload', 'Feature']].sort_values(by='Workload').index
    previous_values = previous_df[['Workload', 'Feature']].sort_values(by='Workload').values
    
    changed_indices = []
    removed_indices = []
    
    prev_idx = 0
    curr_idx = 0
    while True:
        if (curr_idx == len(current_indices)) and (prev_idx == len(previous_indices)):
            break
        elif curr_idx == len(current_indices):
            prev_workload = previous_values[prev_idx][0]
            if prev_workload not in current_values[:,0]:
                removed_indices.append(previous_indices[prev_idx])
                prev_idx += 1
                continue
            else:
                raise Exception('workload error')
            break
        elif prev_idx == len(previous_indices):
            curr_workload = current_values[curr_idx][0]
            curr_feature = current_values[curr_idx][1]
            if curr_workload not in previous_values[:,0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
                continue
            else:
                raise Exception('workload error')
            break
            
        prev_workload = previous_values[prev_idx][0]
        prev_feature = previous_values[prev_idx][1]
        curr_workload = current_values[curr_idx][0]
        curr_feature = current_values[curr_idx][1]
        
        if prev_workload != curr_workload:
            if curr_workload not in previous_values[:,0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
            elif prev_workload not in current_values[:,0]:
                removed_indices.append(previous_indices[prev_idx])
                prev_idx += 1
            else:
                raise Exception('workload error')
        else:
            if prev_feature != curr_feature:
                changed_indices.append(current_indices[curr_idx])
            curr_idx += 1
            prev_idx += 1
    
    changed_df = current_df.loc[changed_indices].drop(['Workload', 'Feature'], axis=1)
    removed_df = previous_df.loc[removed_indices].drop(['Workload', 'Feature'], axis=1)
    for col in feature_cols:
        removed_df[col] = 0

    # removed_df have one more column, 'Ceased'
    removed_df['Ceased'] = True
    return changed_df


def date_range(start, end):
    delta = end - start
    days = [start + timedelta(minutes=i*10) for i in range(delta.seconds//60//10 + 1)]
    return days


def time_format(timestamp):
    return 'T'.join(str(timestamp).split())


days = date_range(start_date, end_date)

perf_start_total = time.time()
perf_start = time.time()

start_end_time_process_list = []

day_df = tsquery.get_timestream(start_date, end_date)

frequency_map = {'<5%': 3.0, '5-10%': 2.5, '10-15%': 2.0, '15-20%': 1.5, '>20%': 1.0}
day_df = day_df.replace({'IF': frequency_map})
day_df['SPS'] = day_df['SPS'].astype(int)
day_df['SpotPrice'] = day_df['SpotPrice'].astype(float)
day_df['SpotPrice'] = day_df['SpotPrice'].round(5)

print(f"elapsed time - single day query: {time.time() - perf_start}")
day_df['OndemandPrice'] = (100 * day_df['SpotPrice']) / (100 - day_df['Savings'])
day_df['OndemandPrice'] = day_df['OndemandPrice'].astype(float)
day_df['OndemandPrice'] = day_df['OndemandPrice'].round(5)

day_timestamps = sorted(list(day_df['time'].unique()))
for timestamp in day_timestamps:
    perf_start = time.time()
    current_df = day_df[day_df['time'] == timestamp].copy()
    print(f"elapsed time - select by time: {time.time() - perf_start}")
    if SAVE_FILENAME not in os.listdir('./'):
        save_gz_s3(current_df, timestamp)
        tsupload.upload_timestream(current_df)
    else:
        perf_start = time.time()
        previous_df = pd.read_csv(SAVE_FILENAME, compression='gzip', header=0, sep=',', quotechar='"')
        save_gz_s3(current_df, timestamp)
        print(f"elapsed time - read and save: {time.time() - perf_start}")
        perf_start = time.time()
        changed_df = compare_nparray(previous_df, current_df, workload_cols, feature_cols)
        print(f"elapsed time - compare: {time.time() - perf_start}")
        perf_start = time.time()
        # changed_df and removed_df have different shape, because of 'Ceased' column
        tsupload.upload_timestream(changed_df)
        print(f"elapsed time - upload: {time.time() - perf_start}")
print(f"elapsed time - total: {time.time() - perf_start_total}")
