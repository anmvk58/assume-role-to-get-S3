import os
import glob

# This function allow remove parquet files in a directory
def remove_parquet_files(directory):
    files = glob.glob(os.path.join(directory, '*.parquet'))
    for file in files:
        os.remove(file)

# Handle
def replace_date(file_name, data_date):
    return file_name.replace('YYYYMMDD', data_date).\
                replace('YYYYMM', data_date[:6]).\
                replace('YYYY', data_date[:4])