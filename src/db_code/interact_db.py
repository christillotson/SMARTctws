import os
import sqlite3
import numpy as np
import pandas as pd

from src.db_code.HumbleNishiyama import HNDB

'''
This file is designed to re-work interaction with the database 
as created with base_db_weird_pandas.py and HumbleNishiyama.py
into simple functions to execute. The database can 
be created from an existing empty csv, 
have new data loaded,
and read into a pandas DataFrame object, to be printed or saved to a csv. 
'''

PATH_TO_DB = os.path.join('src','db_code','database','HN_DB')
PATH_TO_EMPTY = os.path.join('src','db_code','empty_hn_data.csv')

def create_empty(path_string = PATH_TO_DB, path_to_empty = PATH_TO_EMPTY) -> None:

    if os.path.exists(path_string):
        os.remove(path_string)
        print(f"File '{path_string}' deleted successfully.")
    else:
        print(f"File '{path_string}' does not exist.")

    table_name_constant = ['tScore']

    table_create_constant = [
    """
    CREATE TABLE tScore ( 
        p1choice TEXT NOT NULL,
        p2choice TEXT NOT NULL,
        p1_win_cards INTEGER NOT NULL,
        p2_win_cards INTEGER NOT NULL,
        draw_cards INTEGER NOT NULL,
        p1_win_tricks INTEGER NOT NULL,
        p2_win_tricks INTEGER NOT NULL,
        draw_tricks INTEGER NOT NULL,
        times_run INTEGER NOT NULL,
        PRIMARY KEY (p1choice, p2choice)
    )
    ;"""
    ]

    empty_df = pd.read_csv(path_to_empty)

    db = HNDB(
        path = path_string,
        data_DF = empty_df,
        create = True,
        load_new_data = True,
        list_of_table_names_constant=table_name_constant,
        list_of_create_sqls_constant=table_create_constant
        )
    return

def add_new(data_DF:pd.DataFrame,
            path_string = PATH_TO_DB) -> None:

    db = HNDB(
        path = path_string,
        data_DF = data_DF,
        create = False,
        load_new_data = True,
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