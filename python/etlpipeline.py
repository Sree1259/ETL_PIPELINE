import pandas as pd
import os
from utils import FileUtils
from dotenv import load_dotenv
load_dotenv()
import datetime
import uuid
from connect_to_db import get_db_connection
from sqlalchemy import text



logger = FileUtils.set_logger("etl-pipeline")
path = os.getenv("LOCAL_PATH")
def extract_to_df(path, filename):
    try:
        df = pd.read_csv(os.path.join(path, filename))# mainn
        df = FileUtils.remove_whitespaces(df)
        logger.info(f"Loaded file-- {filename} succesfully")
        return df
    except Exception as ex:
        logger.error(f"Error Loading file {filename} : {ex}")


def transform(df,path):
    try:
        logger.info("starting transfromation for employee dataset")
        run_id = str(uuid.uuid4())
        load_time = pd.Timestamp.now()

        
        df['exit_date'].fillna(pd.Timestamp.today().date(), inplace = True)# inplace is used for modification of above/old_df - line 36
        cols = ['manager_id', 'employee_id', 'performance_score', 'salary', 'age']
        df[cols].apply(lambda x : x.fillna(0).astype(int))
        df['work_from_home'].astype(bool)
        df['gender'].replace({
        'F' : 'Female',
        'M' : 'Male',
        'O' : 'Other'}, inplace = True)# when to use assignment
        df['is_active'] = df['exit_date'] == pd.Timestamp.today().date()
        df['load_time'] = load_time
        df["run_id"]  = run_id#till_date data, to trace script/data load time(last time when the data is loaded to database)
        # print(df['load_time'])

        transformed_emp = f"{path}/transformed_employee.csv" 
        df.to_csv(transformed_emp, index = False)# doesn't create a new column index
        logger.info(f"Tranforming file-- file transformation succesfully")#logger file success
        return transformed_emp
    except Exception as e:
        logger.error(f"Transforming File - Failed {e}")

def load_to_db(df, table_name, schema):
    try:
        # schema = os.getenv("SCHEMA")
        engine = get_db_connection() # to redirect to the function: click ctrl+click on the function
        # Load to db
        # truncate_table =text(f'TRUNCATE TABLE "{schema}"."{table_name}"')
        # connection.execute(truncate_table)
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        logger.info(f"Loaded data to table {schema}.{table_name} successfully.")
        
    except Exception as ex:
        logger.error(f"Load to DB failed: {ex}")


def main():
    """key, value = csv , table_name"""
    files = {
        "employee.csv" : "employee",
        "departments.csv" : "departments",
        "projects.csv" : "projects"
    }
    schema = os.getenv("SCHEMA")

    emp_df = extract_to_df(path, "employee.csv")
    transform(emp_df, path)
    transformed_emp_df = pd.read_csv(f"{path}transformed_employee.csv")
    load_to_db(transformed_emp_df,  "employee", schema)

    for filename, table_name in files.items():
        if filename == "employee.csv":
            continue
        df = extract_to_df(path, filename)
        load_to_db(df, table_name, schema)




if __name__ == "__main__": #efficient way, starts the execution
    main()

