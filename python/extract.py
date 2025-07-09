# import pandas as pd
# try:   
#     def load():
#         data_load = pd.read_csv('C:/Users/sreel/Documents/etl_pipeline/employee.csv')
#         print(data_load.head(10))
#         return None
# except as e:

# load()

import pandas as pd
import os
from utils import FileUtils
from dotenv import load_dotenv
load_dotenv()

# path = "C:/Users/bhagh/Downloads/Datasets/employee.csv"
logger = FileUtils.set_logger("etl-pipeline")
path = os.getenv("LOCAL_PATH")
def load_dataframe(path):
    try:
        file_name = '/employee.csv'
        df = pd.read_csv(path+file_name)
        # df = pd.read_csv("C:/Users/sreel/Documents/etl_pipeline/employee.csv")
        logger.info(f"Loading file-- file loaded succesfully")
        # print(df.head(10))
        return df
    except Exception as ex:
        logger.error(f"Error Loading file {ex}")
load_dataframe(path)


