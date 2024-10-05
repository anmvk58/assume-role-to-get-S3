import os
import sys
import requests
import json
import logging
import boto3
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

data_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
# data_date = '20230811'

# Prepare Logger to write in console and file
logFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

fileHandler = logging.FileHandler(filename='logs/log_run_{}.log'.format(data_date))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

# Load config
config = json.loads(open("config/config_dev.json", "r").read())
open("config.json", "r").close()

# boto3 S3:
sts_client = boto3.client(service_name='sts',
                          region_name='ap-southeast-1',
                          endpoint_url='https://sts.{}.amazonaws.com'.format(config.get("region"))
                          )

# ARN and a role session name
assumed_role_object = sts_client.assume_role(
    RoleArn=config.get("role_assume"),
    RoleSessionName="AssumeRoleSession"
)
credentials = assumed_role_object['Credentials']

# STS rotate role when use across account
resource = boto3.resource(service_name='s3',
                          aws_access_key_id=credentials['AccessKeyId'],
                          aws_secret_access_key=credentials['SecretAccessKey'],
                          aws_session_token=credentials['SessionToken'],
                          region_name=config.get("region"),
                          endpoint_url='https://s3.{}.amazonaws.com'.format(config.get("region"))
                          )
client_bucket = resource.Bucket(config.get("BUCKET_NAME"))

if __name__ == '__main__':
    logger.info("\n*********************************")

    # Get list file to process:
    list_file = config.get("list_file")
    logger.info("List File Config: " + str(list_file))

    # Get list file pull success today:
    try:
        list_pulled_file = open("results/result_{}.txt".format(data_date), "r").read().split("\n")
        open("results/result_{}.txt".format(data_date), "r").close()
        logger.info("List File Pulled: " + str(list_pulled_file))
    except Exception:
        list_pulled_file = []

    # Make list file to pull:
    list_to_do = [{"file": i.get("file").replace('YYYYMMDD', data_date), "path": i.get("path")} for i in list_file if
                  i.get("file").replace('YYYYMMDD', data_date) not in list_pulled_file]
    logger.info("List File To Do: " + str(list_to_do))

    if len(list_to_do) == 0:
        logger.info("Done all file ! Data Date = " + data_date)
        sys.exit(0)

    # Check Source
    # 1. Generate token to call
    response_token = requests.post(url=config.get("token_url"), headers=config.get("token_header"), verify=False)
    if response_token.status_code == 200:
        access_token = json.loads(response_token.text).get("access_token")
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        logger.info("Get access_token Success !\n")
    else:
        logger.info("Get access_token Failed !\n")
        sys.exit()

    # Loop in list file have to pull:
    for item in list_to_do:
        # 2. CAll API check Source:
        file_name = item.get("file")
        path = item.get("path")

        if "*" in file_name:
            downloaded = False
            index = 0
            while True:
                file_name_pull = file_name.replace("*", str(index))
                body = {'table_name': 'file|' + file_name_pull}
                response = requests.post(url=config.get("check_source_url"), json=body, headers=headers, verify=False)
                logger.info("-----------------------------")
                index += 1
                if response.status_code == 200:
                    logger.info("Call API check source for [" + file_name_pull + "] success!")

                    #

                    if len(json.loads(response.text).get("successed_tables")) >= 1:
                        logger.info("Source is READY for: [" + file_name_pull + "]")

                        # download file:
                        data_date_path = "{:year=%Y/month=%m/day=%d}".format(datetime.strptime(data_date, '%Y%m%d'))
                        key_file = "ERP_{}/{}/{}".format(file_name.replace(data_date, 'YYYYMMDD').replace('*','0'), data_date_path,
                                                         file_name_pull)
                        try:
                            client_bucket.download_file(key_file, '{}{}'.format(path, file_name_pull))
                            logger.info("Download file successfully for: [" + file_name_pull + "]")
                            downloaded = True

                            if file_name_pull.split(".")[-1].upper() == 'ZIP':
                                os.system("unzip -o  {PATH}{FILE} -d {PATH}".format(PATH=path, FILE=file_name_pull))
                                logger.info("Unzip successfully for: [" + file_name_pull + "]")
                        except Exception as e:
                            logger.info("Download file FAILED for: [" + file_name_pull + "]")
                            logger.error("###ERROR###: " + str(e))
                    else:
                        logger.info("Source is NOT Ready for: [" + file_name_pull + "]")
                        break
                else:
                    logger.info(
                        "Call API check source for [" + file_name + "] FAILED ! ERROR: " + str(response.status_code))

            # Write to results run today:
            if downloaded == True:
                open("results/result_{}.txt".format(data_date), "a").write(file_name + '\n')
                open("results/result_{}.txt".format(data_date), "a").close()
                logger.info("Write result log successfully for: [" + file_name + "]")

        else:
            body = {'table_name': 'file|' + file_name}
            response = requests.post(url=config.get("check_source_url"), json=body, headers=headers, verify=False)
            logger.info("-----------------------------")
            if response.status_code == 200:
                logger.info("Call API check source for [" + file_name + "] success!")

                # If source ready then pull file
                if len(json.loads(response.text).get("successed_tables")) >= 1:
                    logger.info("Source is READY for: [" + file_name + "]")

                    # download file:
                    data_date_path = "{:year=%Y/month=%m/day=%d}".format(datetime.strptime(data_date, '%Y%m%d'))
                    key_file = "ERP_{}/{}/{}".format(file_name.replace(data_date, 'YYYYMMDD'), data_date_path, file_name)
                    try:
                        client_bucket.download_file(key_file, '{}{}'.format(path, file_name))
                        # client_bucket.download_file(key_file, file_name)
                        logger.info("Download file successfully for: [" + file_name + "]")

                        if file_name.split(".")[-1].upper() == 'ZIP':
                            os.system("unzip -o {PATH}{FILE} -d {PATH}".format(PATH=path, FILE=file_name))
                            logger.info("Unzip successfully for: [" + file_name + "]")

                        # write to results run today:
                        open("results/result_{}.txt".format(data_date), "a").write(file_name + '\n')
                        open("results/result_{}.txt".format(data_date), "a").close()
                        logger.info("Write result log successfully for: [" + file_name + "]")
                    except Exception as e:
                        logger.info("Download file FAILED for: [" + file_name + "]")
                        logger.error("###ERROR###: " + str(e))
                else:
                    logger.info("Source is NOT Ready for: [" + file_name + "]")

            else:
                logger.info("Call API check source for [" + file_name + "] FAILED ! ERROR: " + str(response.status_code))
        logger.info("")