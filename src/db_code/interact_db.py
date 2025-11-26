import os
import sqlite3
import numpy as np
import pandas as pd

from db_code.CWFAC_db import CWFACDB

'''
This file is designed to re-work interaction with the database 
into simple functions to execute. The database can 
have new data loaded though the webscraping script,
and read into a pandas DataFrame object through querying.

Creation of a new database would require manual deletion of the old one,
and then manual creation by passing in new data and setting create = True.
I will NOT give an easy to use function to do that here, that must be done manually
to prevent user error.
'''

PATH_TO_DB = os.path.join('db_code','databasefile')

def add_new(data_DF:pd.DataFrame,
            path_string = PATH_TO_DB) -> None:

    db = CWFACDB(
### UNFINISHED WAS HERE LAST 11/19/2025 1:50 PM
        )
    
    return

def read_db(path_string = PATH_TO_DB) -> pd.DataFrame:

    db = HNDB(
        path = path_string,
        create = False,
        load_new_data = False,
        )

    sql = """
    SELECT * FROM tScore
    ;"""

    pd.set_option('display.max_rows', 100) # max just needs to be higher than 56 data + 1 column that is going on for this implementation
    
    query_run = db.run_query(sql)

    return(query_run)