import os
import sys
import uuid
import requests
import json
import logging
import boto3
import warnings
from datetime import datetime, timedelta
from utils.parquet_2_csv import process_parquet_2_csv
from utils.utils import remove_parquet_files, replace_date
from utils.check_source_utils import SinglePull
from utils.logger_config import setup_logging

warnings.filterwarnings("ignore")

data_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
data_date = '20240725'

# Prepare Logger to write in console and file
# Removed because utils.logger_config instead

# logFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# fileHandler = logging.FileHandler(filename='logs/log_run_{}.log'.format(data_date))
# fileHandler.setFormatter(logFormatter)
# logger.addHandler(fileHandler)

# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(logFormatter)
# logger.addHandler(consoleHandler)

# Load config
config = json.loads(open(file="config/config_pull_dev.json", mode="r", encoding="utf8").read())
open(file="config/config_pull_dev.json", mode="r", encoding="utf8").close()

# boto3 S3:
sts_client = boto3.client(service_name='sts',
                          region_name='ap-southeast-1',
                          endpoint_url='https://sts.{}.amazonaws.com'.format(config['region'])
                          )

# ARN and a role session name
assumed_role_object = sts_client.assume_role(
    RoleArn=config['role_assume'],
    RoleSessionName='AssumeRoleSession'
)
credentials = assumed_role_object['Credentials']

# STS rotate role when use across account
resource = boto3.resource(service_name='s3',
                          aws_access_key_id=credentials['AccessKeyId'],
                          aws_secret_access_key=credentials['SecretAccessKey'],
                          aws_session_token=credentials['SessionToken'],
                          region_name=config['region'],
                          endpoint_url='https://s3.{}.amazonaws.com'.format(config['region'])
                          )

if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger("Anmv_Assume_Role_App")
    logger.info("\n*********************************")

    # Get list file to process:
    list_dict_file = config['list_file']
    list_file_cfg = [item['file_out_name'] for item in list_dict_file]
    logger.info("List File Config: " + str(list_file_cfg))

    # Get list file pull success today:
    try:
        list_pulled_file = open("results/result_{}.txt".format(data_date), "r").read().split("\n")
        open("results/result_{}.txt".format(data_date), "r").close()
    except Exception:
        list_pulled_file = []
    logger.info("List File Pulled: " + str(list_pulled_file))

    # Make list file need to pull today:
    list_to_do = [item for item in list_dict_file if
                  replace_date(item['file_out_name'], data_date) not in list_pulled_file]
    list_to_do_name = [replace_date(item["file_out_name"], data_date) for item in list_to_do]
    logger.info("List File To Do: " + str(list_to_do_name))

    if len(list_to_do) == 0:
        logger.info("Done all file ! Data Date = " + data_date)
        sys.exit(0)

    # Get all Type Pull (such as EOD/AppEgn):
    for item in config['pull_type'].keys():
        SinglePull(item, config['pull_type'][item]['check_source_url'], config['pull_type'][item]['bucket_name'])
        logger.info(f"Load config Type [{item}] Success")
    # End of load Type Pull

    # Check Source
    # 1. Generate token to call api check source
    response_token = requests.post(url=config['token_url'], headers=config.get("token_header"), verify=False)
    if response_token.status_code == 200:
        access_token = json.loads(response_token.text).get("access_token")
        headers = {
            'Authorization': 'Bearer {}'.format(access_token),
            'x-request-id': str(uuid.uuid4())
        }
        logger.info("Get access_token Success !\n")
    else:
        logger.info("Get access_token Failed !\n")
        sys.exit(1)

    # Loop in list file have to pull:
    for item in list_to_do:
        # 2. CAll API check Source:
        # This part use for check source ready
        file_name = replace_date(item['file_out_name'], data_date)

        key_check_src = item['key_check_source']
        body = {
            'tablename': key_check_src,
            'datadate': int(data_date)
        }

        logger.info("-----------------------------")
        # If source ready then pull file
        if SinglePull.instances[item['pull_type']].process_check_source(body, headers, logger, file_name):
            logger.info("Source is READY for: [" + file_name + "]")

            # download file:
            data_date_path = "{:year=%Y/month=%m/day=%d}".format(datetime.strptime(data_date, '%Y%m%d'))
            key_file = item['s3_path_in'].replace('PATTERN_DATE', data_date_path)

            try:
                # remove file old parquet for run again
                remove_parquet_files(item['path_out'])

                # loop through files in that prefix
                client_bucket = resource.Bucket(SinglePull.instances[item['pull_type']].get_bucket_name())
                list_key = list(
                    filter(lambda x: x.key.endswith(item['file_extension']),
                           client_bucket.objects.filter(Prefix=key_file)))
                # logger.info('List key in S3: '.format(list_key))

                # Create folder if not exist
                os.makedirs(item['path_out'], exist_ok=True)

                # Load file from S3 to target dir
                for index, obj in enumerate(list_key):
                    client_bucket.download_file(obj.key,
                                                '{}{}_{}.{}'.format(item['path_out'], file_name.split('.')[0],
                                                                    index, item['file_extension']))
                logger.info("Download file successfully for: [" + file_name + "]")

                # Convert from parquet to CSV
                if item['file_extension'].lower() == 'parquet':
                    process_parquet_2_csv(item, data_date)
                    logger.info("Convert and merge from parquet to CSV successfully for: [" + file_name + "]")
                    # Remove parquet file after done all
                    remove_parquet_files(item['path_out'])

                # File type csv, just download
                elif item['file_extension'].lower() == 'csv':
                    logger.info("Download and merge to CSV successfully for: [" + file_name + "]")

                # For another type like zip, gz v..v
                else:
                    logger.info("Unsupported file type: [" + item['file_extension'].lower() + "], just download !")

                # Write to results run today:
                open("results/result_{}.txt".format(data_date), "a").write(file_name + '\n')
                open("results/result_{}.txt".format(data_date), "a").close()
                logger.info("Write result log successfully for: [" + file_name + "]\n")

            except Exception as e:
                logger.error("Process FAILED for: [" + file_name + "]")
                logger.error("###ERROR###: " + str(e))

        else:
            logger.info("Source is NOT Ready for: [" + file_name + "]")

        logger.info("")

    logger.info("Finish loop file to pull !\n")