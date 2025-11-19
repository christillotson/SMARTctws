# be careful with copy and pasting
# basically in here go through billboard.py and get through everything except body of function

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
    '''

    def __init__(self,
                 path: str,
                 create: bool = False
                ):
        '''
        Arguments
            path: The path to the database file

            create: If the databse does not exist, it will be created
                    if this is set to True, otherwise a FileNotFound
                    error will be raised.
        '''
        self.path = path # needs to occur BEFORE things that use it, such as self._check_exists
        #set to not connected by default
        self._connected = False

        #check if the database exists by seeing what we passed into create
        self._check_exists(create)

        if not self._existed:
            self._create_tables()
            # self._load_data() # for future databses probably would just have load data be called
            # whenever have new data to load into databse, so wouldn't include create tables AND
            # load data

        return

    def _create_tables(self) -> None:

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
        # format of following for every table
        # tSong = pd.read_csv(self.PATH_SONG)
        # where PATH_SONG is like 
        # PATH_SONG = os.path.join('data','bb_song.csv')
        # sql = """
        #     INSERT INTO tSong (song_id, year, artist, track, time)
        #     VALUES (:song_id, :year, :artist, :track, :time)
        #     ;"""
        # for row in tSong.to_dict(orient='records'):
        #     self.run_action(sql, row, keep_open=True)

        # then run this last line last
        # self._commit_and_close()
        return

    def _connect(self,
                  foreign_keys: bool = True) -> None:
        '''
        Establish a connection to the databse and create a cursor. If
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
        Check if the databsae and all folders in the path exist.

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


        # as playing around with databases moving forward could add more code, but this is bare minimum, almost all is in billboard
        # dont just copy and paste from billboard.py