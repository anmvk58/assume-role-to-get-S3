import logging
from datetime import datetime, timedelta

def setup_logging():
    """Setup logging configuration with dynamic log file name based on current date"""
    data_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    log_filename = 'logs/log_run_{}.log'.format(data_date)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
