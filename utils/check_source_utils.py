import json
import requests


# Class for check source utils
class CheckPullType:
    def __init__(self, chk_type, check_source_url, bucket_name):
        self.__type = chk_type
        self.__check_source_url = check_source_url
        self.__bucket_name = bucket_name

    def get_check_source_url(self):
        return self.__check_source_url

    def get_bucket_name(self):
        return self.__bucket_name

    def process_check_source(self, body, headers, logger, file_name):
        response = requests.post(url=self.__check_source_url, json=body, headers=headers, verify=False)

        try:
            # call api oke
            if response.status_code == 200:
                logger.info("Call API check source for [" + file_name + "] success!")
                # If source ready then pull file
                if len(json.loads(json.loads(response.text).get("succeeded_tables"))) >= 1:
                    return True
            return False

        except Exception as e:
            logger.error("### Call API check source for [" + file_name + "] NOT OKE !")


# For create singleton of PullType
class SinglePull:
    instances = {}

    def __init__(self, chk_type, check_source_url, bucket_name):
        if chk_type not in SinglePull.instances.keys():
            SinglePull.instances[chk_type] = SinglePull.create_check_pull_type(chk_type, check_source_url, bucket_name)

    @staticmethod
    def create_check_pull_type(chk_type, check_source_url, bucket_name):
        return CheckPullType(chk_type, check_source_url, bucket_name)
