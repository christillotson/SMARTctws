import os
import sqlite3
import numpy as np
import pandas as pd

# telling sqlite when you see something of first datatype, do this function
sqlite3.register_adapter(np.int64, lambda x: int(x))
# so when see np.int64 turn into native python int
sqlite3.register_adapter(np.int32, lambda x: int(x))
# above two are required for sqlite to properly interpret numpy integers

class BaseDB:
    '''
    This class contains code that can be used with any sqlite database. Or at least, that's what it's designed to do.

    Implement a _load_new_data method to make a continuously updating database.
    '''

    def __init__(self,
                 path: str,
                 data_DF:pd.DataFrame = pd.DataFrame(),  # creating as empty because for reading don't need it
                 # but for writing and creating (creating really just writing with empty csv) should include
                 
                 create: bool = False,
                 load_new_data:bool = False,

                 list_of_table_names_constant: list = [],
                 list_of_table_paths_constant: list = [],
                 list_of_create_sqls_constant: list = [],
                ):
        '''
        Arguments
            path: The path to the database file

            data_DF: The data that is added (if creating from the empty csv or adding new data)
            By default an empty pandas dataframe because it's not necessary to pass in when being read

            create: If the database does not exist, it will be created
                    if this is set to True, otherwise a FileNotFound
                    error will be raised.

            load_new_data: Whether or not new data is being loaded

            *The following three are associated with each table and should be of the same
            index within each list. Each are for tables that are constant (do not change)*
            
            list_of_table_names_constant: A list of table names, preferably in the format of tName.

            list_of_table_paths_constant: A list of the paths where the CSVs used for creating each table are stored.
            Suggested to create these with os.path.join to improve compatibility, so will need to
            import os in the notebook or other environment to create them before calling DB class.
            
            list_of_create_sqls: The sql commands to execute to create each table. Includes tables which will be changed.       
        '''

        self.path = path 
        self.data_DF = data_DF

        # creating lists
        self.list_of_table_names = list_of_table_names_constant
        self.list_of_table_paths = list_of_table_paths_constant
        self.list_of_create_sqls = list_of_create_sqls_constant
        
        # needs to be in format of:
        # sql = """
        #     CREATE TABLE t(table name) (
        #     primary_key_row TYPE PRIMARY KEY
        #     ETCETERA
        #     )

        # ;"""
        # self.run_action(sql)    

        self._connected = False

        #check if the database exists by seeing what we passed into create
        self._check_exists(create)

        if not self._existed:
            self._create_tables()
            # self._load_data() # for future databases probably would just have load data be called
            # turning this off because no data needs to be initially loaded?
            # whenever have new data to load into database, so wouldn't include create tables AND
            # load data

        
        if load_new_data == True:
            self._load_new_data()

        return

    def _load_new_data(self) -> None:
        # implement in new py file
        return

    def _create_tables(self) -> None:
        for create_command in self.list_of_create_sqls:
            self.run_action(create_command)

        # needs to be in format of:
        # sql = """
        #     CREATE TABLE t(table name) (
        #     primary_key_row TYPE PRIMARY KEY
        #     ETCETERA
        #     )

        # ;"""
        # self.run_action(sql)

        return

    def _load_data(self) -> None:
        # this implements globals() function. Look up geeksforgeeks.org for source.
        for i in range(len(self.list_of_table_names)):
            globals()[self.list_of_table_names[i]] = pd.read_csv(self.list_of_table_paths[i])
            # first bit before equals sign turns the string into a name

            columns = ', '.join(globals()[self.list_of_table_names[i]].columns) # easy to get columns

            values_list = [] # will store column names with colons here basically
            for val in globals()[self.list_of_table_names[i]].columns:
                val_with_colon = f':{val}' # put the colon before it
                values_list.append(val_with_colon) # add to list of values           
            values = ', '.join(f'{val}' for val in values_list) # finally make values string which will be put in sql command below
            table_name = self.list_of_table_names[i]
            sql = f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({values})
                ;"""

            # this is called ORM object relational mode
            # creating a system where dont have to do sql myhself
            # just avoid publicly facing database f strings because can be insterted by public

            for row in globals()[self.list_of_table_names[i]].to_dict(orient = 'records'):
                self.run_action(sql, params = row, keep_open = True)
        
        self._commit_and_close()

        
        return

    def _connect(self,
                  foreign_keys: bool = True) -> None:
        '''
        Establish a connection to the database and create a cursor. If
        a connection is already opened, will not open a new one.

        If foreign_keys is set to False, foreign key constraints will
        not be enabled
        '''

        if not self._connected:
            self._conn = sqlite3.connect(self.path)
            self._curs = self._conn.cursor()
            if foreign_keys:
                self._curs.execute("PRAGMA foreign_keys=ON;") # pragma is like a database rule
                # never want more than one connection at once is really bad will get in trouble so... is within this if loop
            self._connected = True
        return

    def _close(self) -> None:
        '''
        Close the database connection.
        '''
        self._conn.close()
        self._connected = False
        return

    def _commit_and_close(self) -> None:
        self._conn.commit()
        self._close()
        return

    def _check_exists(self,
                      create: bool
                     ) -> None:
        '''
        Check if the database and all folders in the path exist.

        If create is False, raise a FileNotFound error if the database,
        or any of the folders in the path do not exist.

        If create is True, then create any needed folders as well as the
        database file.
        '''
        # Assume the database existed,
        # set to False later if it did not
        self._existed = True

        # Parse out filename into the individual directories and database file
        path_parts = self.path.split(os.sep) # split is just a string method to split into substrings


        n = len(path_parts)
        for i in range(n):
            part = os.sep.join(path_parts[:i+1])
            if not os.path.exists(part):
                self._existed = False # doesnt exist so should i be creating things
                if not create:
                    raise FileNotFoundError(f'{part} does not exist')
                else:
                    if i == n-1:
                        # We need to create the database file
                        print('Creating DB')
                        self._connect()
                        self._close()
                    else: 
                        os.mkdir(part) # may lead to errors later if part is file and not folder, check back later to see if this is issue and this is location of issue, idk how would fix though
                        
        return

    def run_query(self,
                  sql: str,
                  params: dict = None, # optional dictionary parameters
                  keep_open: bool = False
                 ) -> pd.DataFrame:
        '''
        Arguments
            sql: A string containing SQL code
            params: Optional dictionary of query parameters
            keep_open: If True, database connection will remain open
                        after running the query (default is False).

        Returns a pandas DataFrame containing query results.
        '''
        # need to connect to database before running query
        self._connect()

        try:
            results = pd.read_sql(sql, self._conn, params = params)
        except Exception as e:
            raise type(e)(f'sql: {sql}\n params: {params}')
        finally:
            if not keep_open:
                self._close()
        return results
        
    def run_action(self,
                   sql: str,
                   params: dict = None,
                   commit: bool = False,
                   keep_open: bool = False
                  ) -> int:
        '''
        Arguments
            sql: A string containing SQL code
            params: Optional dictionary of query parameters
            commit: If True, changes will be immediately committed
            keep_open: If True, database connection will remain open
                        after running the query (default is False).

        Returns the lastrowid property of the cursor (which is the row id
        of the most recently modified row)
        '''
        # connect to database
        self._connect()
        try:
            if params is None:
                self._curs.execute(sql)
            else:
                self._curs.execute(sql, params)
            if commit:
                self._conn.commit()
        except Exception as e:
            # If anything goes wrong, rollback and close
            self._conn.rollback()
            self._close()
            raise type(e)(f'sql: {sql}\nparams: {params}') from e

        if not keep_open:
            self._close()
        return self._curs.lastrowid