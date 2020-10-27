from . import *
from .connection import PostgresConnectionHandler
import uuid
from django.db import DatabaseError, transaction
from dateutil.parser import parse
from psycopg2 import ProgrammingError


# %% Postgres Connector
class Postgres(PostgresConnectionHandler):
    column_description = [
        'name',
        'type_code',
        'display_size',
        'internal_size',
        'precision',
        'scale',
        'null_ok'
    ]
    ColumnData = collections.namedtuple('ColumnData', ' '.join(column_description))

    def __init__(self):
        """
        Snowflake inherits ConnectionHandler from BI.data_warehouse.connection
        This class interacts with Snowflake and handles query executions
        """
        PostgresConnectionHandler.__init__(self)
        self.column_data = []
        self.column_names = []
        self.query_results = []
        self.query_with_header = []

        self.column_map = []

        self.query = None
        self.overwrite = False
        self.date_time_format = None
        self._meta_data_col = None

        self.table_name = None
        self.insert_data_header = []
        self.insert_data = []
        self.data_width = None
        self.data_length = None
        self.upload_chunks = []
        self.temp_csv_file_name = None

        self.instance_created = False

    def _reset(self):
        """
        Resets Instance variables
        """
        self.column_data = []
        self.column_names = []
        self.query_results = []
        self.query_with_header = []
        self.column_map = []
        self.query = None
        self.overwrite = False
        self.date_time_format = None
        self._meta_data_col = None
        self.table_name = None
        self.insert_data_header = []
        self.insert_data = []
        self.data_width = None
        self.data_length = None
        self.upload_chunks = []
        self.temp_csv_file_name = None
        self.instance_created = False

    def _format_results(self):
        """
        Converts the List of Tuples returned from a query to a List of Lists
        """
        for i, row in enumerate(self.query_results):
            if isinstance(self.query_results[i], tuple):
                self.query_results[i] = list(self.query_results[i])

    def _update_column_size(self, column, new_size):
        """
        Updates a VARCHAR column type to larger byte size
        E.G. VARCHAR(10) to VARCHAR(80)
        :param column: Name of the VARCHAR column
        :param new_size: The new byte size for the column
        """
        query = """
        ALTER TABLE {table}
        ALTER COLUMN {column} VARCHAR({size})
        """.format(table=self.table_name, column=column, size=new_size)
        self.execute_sql_command(query)
        self.get_columns()

    def _create_temp_csv(self):
        """
        Prepares insert_data for insert and writes the data into a temporary csv for staging.
        """
        #  Prep data
        self.prepare_data_for_insert()
        try:
            #  Create random file name for temp csv to avoid conflicts with threading
            self.temp_csv_file_name = uuid.uuid4().hex + '.csv'
            #  Create csv all settings are Snowflake default settings
            with open(self.temp_csv_file_name, 'w+', encoding='utf-8') as temp_csv:
                writer = csv.writer(temp_csv, delimiter=',', lineterminator='\n',
                                    quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
                for row in self.insert_data:
                    writer.writerow(row)
            #  Insert temp csv into table
            self.insert_csv_into_table(self.table_name, temp_csv.name, overwrite=self.overwrite)
        except Exception as e:
            raise e
        finally:
            #  Delete temp csv
            os.remove(temp_csv.name)

    def prepare_data_for_insert(self):
        """
        Prepares data for an insert statement by comparing the column
        to the column types in the table and making sure that each column
        is a consistent type.  Will set empty strings to None, which behaves
        as a NULL value.
        """
        #  Check if _meta_data_col is needed.  If it is needed,
        #  find the column index in the table and create a timestamp
        if self._meta_data_col:
            meta_col_index = max(i for i, col in enumerate(self.column_names) if col.lower() == '_meta_report_time')
        timestamp = dt.datetime.now().replace(minute=0, second=0, microsecond=0)
        #  Loop through all data being inserted
        for i, row in enumerate(self.insert_data):
            #  Checks that data is not a tuple, converts to list if needed
            if isinstance(self.insert_data[i], tuple):
                self.insert_data[i] = list(self.insert_data[i])
            if self._meta_data_col:
                #  Insert timestamp into _meta_data_col index of each row
                if meta_col_index > len(self.insert_data[i]) - 1:
                    self.insert_data[i] = self.insert_data[i].append(timestamp)
                else:
                    self.insert_data[i] = self.insert_data[i].insert(meta_col_index, timestamp)
            # Loop through each column and verify data is ready for column data type
            for j, item in enumerate(self.insert_data[i]):
                #  Get the column dictionary and extract type and scale
                current_column = self.column_data[j]
                column_type = current_column.type_code
                scale = current_column.scale
                internal_size = current_column.internal_size
                column_name = current_column.name
                #  Any empty strings converted to None
                if item == '':
                    self.insert_data[i][j] = None
                if isinstance(item, float):
                    if math.isnan(item):
                        self.insert_data[i][j] = None
                #  Convert Pandas Timestamp type to regular datetime type
                if isinstance(self.insert_data[i][j], pd.Timestamp):
                    self.insert_data[i][j] = self.insert_data[i][j].to_pydatetime()
                #  If data in column process data
                if self.insert_data[i][j]:
                    #  Current Type 0 represents a number data type in Snowflake
                    if column_type == 0:
                        #  Current Scale represnts the number of points following a decimal
                        #  If Scale is larger than 0 convert to float
                        if scale and item is not None:
                            self.insert_data[i][j] = float(item)
                        else:
                            #  If data is a string type convert to int or float based on scale
                            if isinstance(self.insert_data[i][j], str):
                                if ',' in self.insert_data[i][j]:
                                    self.insert_data[i][j] = self.insert_data[i][j].replace(',', '')
                                if '.' in self.insert_data[i][j] and scale == 0:
                                    self.insert_data[i][j] = float(self.insert_data[i][j])
                            self.insert_data[i][j] = int(self.insert_data[i][j])
                    #  Current Type 3 represents a Date type
                    #  Current Type 8 represents a Timestamp_NTZ type
                    elif column_type == 3 \
                            or column_type == 8:
                        # If data is not a datetime type attempt to convert to datetime
                        if not isinstance(self.insert_data[i][j], dt.datetime):
                            try:
                                # If date_time_format is provided convert item from that format
                                if self.date_time_format:
                                    self.insert_data[i][j] = dt.datetime.strptime(self.insert_data[i][j],
                                                                                  self.date_time_format)
                                # If no date_time_format attempt to parse the date
                                else:
                                    self.insert_data[i][j] = parse(self.insert_data[i][j])
                            except:
                                pass
                    elif column_type == 2:
                        if not isinstance(self.insert_data[i][j], str):
                            try:
                                self.insert_data[i][j] = str(self.insert_data[i][j])
                            except:
                                pass
                        if len(self.insert_data[i][j]) > internal_size:
                            size = len(self.insert_data[i][j])
                            self._update_column_size(column_name, size)
        self._get_data_size()

    def _do_insert(self):
        """
        Insert data into table
        """
        if self.console_output:
            print('Inserting Data into {table}'.format(table=self.table_name))
        #  Prep all data for the insert
        self.prepare_data_for_insert()

        #  Create insert statement

        command = 'INSERT INTO {table} '.format(table=self.table_name)
        columns_names = '(' \
                        + ', '.join(self.column_map) if self.column_map else ', '.join(self.column_names) \
                        + ')' \
                        + ' VALUES '
        values = '(' + ', '.join('%s' for i in range(self.data_width)) + ')'
        self.query = command + columns_names
        self.query = self.query.format(table=self.table_name)
        args_str = ','.join(self.cursor.mogrify(values, x) for x in self.insert_data)

        #  If Overwrite is needed perform insert overwrite with first row
        if self.overwrite:
            self._clear_table()
        #  Insert data into table
        self.cursor.execute(self.query + args_str)

    def _do_chunk_insert(self):
        """
        Separate insert data into chunks of 16k rows
        Then insert chunks 1 by 1 into table
        This function is no longer used and has been replace by CSV staging
        """
        #  If Overwrite is needed perform insert overwrite with first row
        if self.overwrite:
            self._clear_table()
            self.overwrite = False
        #  Insert each chunk in to table
        for chunk in self.upload_chunks:
            self.insert_data = chunk.tolist()
            self._do_insert()
        #  Reset Upload Chunks
        self.upload_chunks = []

    def _clear_table(self, table=None):
        """
        Perform Delete Statement on Table
        This is used either seperately with the Table Parameter
        or for when CSV overwrite is marked True
        :param table: The table to clear
        """
        #  Set Table name
        if table:
            self.table_name = table
        #  If overwrite is create delete command and execute command
        if self.overwrite:
            query = 'DELETE FROM {table}'.format(table=self.table_name)
            self.execute_sql_command(query)

    def _format_query_results(self):
        """
        Convert query results from List of Tuples to List of Lists
        """
        for i, row in enumerate(self.query_results):
            if isinstance(row, tuple):
                self.query_results[i] = list(row)

    def _create_meta_report_column(self):
        """
        If _meta_data_col is True
         If the column does not exist, create the column
        """
        query = '''
        ALTER TABLE {table}
        ADD COLUMN _META_REPORT_TIME TIMESTAMP_NTZ
        '''.format(table=self.table_name)
        _meta_col_exists = False
        for x in self.column_names:
            if '_meta_report_time' in x.lower():
                _meta_col_exists = True
                break
        if not _meta_col_exists:
            self.execute_sql_command(query)
            self.get_columns()

    def _get_data_size(self):
        self.data_width = max(len(row) for row in self.insert_data)
        self.data_length = len(self.insert_data)

    def execute_sql_command(self, command, bindvars=list()):
        """
        Execute a sql command that does not return results
        :param command:  command to execute
        :param bindvars:  a list of bind variables
        """
        try:
            #  Open Cursor and Execute Command
            self._reset()
            self.query = command
            self._open_cursor()
            if self.console_output:
                print('Executing Query')
            if bindvars:
                self.cursor.execute(self.query, bindvars)
            else:
                self.cursor.execute(self.query)
            #  Commit changes
            self.connection.commit()
        except DatabaseError:
            transaction.rollback()
        except Exception as e:
            #  Rollback changes if exception is thrown
            self.connection.rollback()
            raise e
        finally:
            #  Close the cursor
            self._close_cursor()

    def execute_query(self, sql, bindvars=None):
        """
        Execute a query and return the results
        :param sql: The query to execute
        :param bindvars: A list of the variables, in the order they should be bound
        """
        try:
            #  Open Cursor and execute query
            self.query = sql
            self._open_cursor()
            if self.console_output:
                print('Executing Query')
            if bindvars:
                self.cursor.execute(self.query, bindvars)
            else:
                self.cursor.execute(self.query)

            if self.console_output:
                print('Retrieving Query Results')
            #  Fetch results and format the results
            try:
                self.query_results = self.cursor.fetchall()
                #  If query is successful record the column names
                if self.cursor.description:
                    self.column_names = [x.name for x in self.cursor.description]
                    self.column_data = list(self.cursor.description)
                    self.query_with_header = [self.column_names] + self.query_results
            except ProgrammingError:
                self.query_results = []
            except Exception as e:
                raise e
            self._format_query_results()

        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            #  Close the cursor
            self._close_cursor()

    def get_columns(self, table_name=None):
        """
        Get the column descriptions and name for a table
        :param table_name: The name of the table to get the column data from
        """
        try:
            #  Open Cursor and execute query
            self._open_cursor()
            if table_name:
                self.table_name = table_name

            self.query = 'SELECT * FROM {table} WHERE 1=0'.format(table=self.table_name)

            self.execute_query(self.query)

        except Exception as e:
            raise e
        finally:
            #  Close the cursor
            self._close_cursor()

    def get_table_data(self, table):
        """
        Perform ' Select * ' on a table to return all results
        :param table: The table to access
        """
        self.table_name = table
        #  Get column data then execute Select * query
        query = 'SELECT * FROM {table}'.format(table=self.table_name)
        self.execute_query(query)

    def insert_into_table(self, table_name, data, columns=[],
                          overwrite=False, date_time_format=None,
                          header_included=False, _meta_data_col=None):
        """
        Insert data into a table
        :param table_name: The table to insert data into
        :param data: The List of Lists of data to insert
        :param columns: If data has less columns than the table the columns included in the table
        :param overwrite: Delete the existing data in the table
        :param date_time_format: If date/datetime strings are included the format they are in (%Y-%m-%d)
        :param header_included: If the first row of data is the header of the data
        :param _meta_data_col: Add timestamp for insert into _meta_report_time col
        """
        if self.instance_created:
            self._reset()

        self.instance_created = True
        self.table_name = table_name
        self.overwrite = overwrite
        self._meta_data_col = _meta_data_col
        if date_time_format:
            self.date_time_format = date_time_format
        self.get_columns()
        if self._meta_data_col:
            self._create_meta_report_column()

        # If header included remove the header for the data
        if header_included:
            self.insert_data_header = data[0]
            del data[0]

        self.column_map = columns
        self.insert_data = data
        self.instance_created = True
        # If data exists in data evaluate data size
        if self.insert_data:
            self._get_data_size()

            if self.data_length > 16000:
                self._create_temp_csv()
            else:
                try:
                    #  Open Cursor and Insert data
                    self._open_cursor()
                    if self.insert_data:
                        self._do_insert()
                    #  Commit Insert
                    self.connection.commit()
                except Exception as e:
                    #  Rollback changes if exception thrown
                    self.connection.rollback()
                    raise e

                finally:
                    #  Reset inser_data and close the cursor
                    self.insert_data = []
                    self._close_cursor()

    def insert_raw_csv_into_table(self, table, file_name, overwrite=False, header_included=False, encoding='utf-8'):
        """
        Insert an existing non-processed csv into a table
        :param table: The name of the table to insert the data into
        :param file_name: The file path including the file
        :param overwrite: Delete the existing data in the table
        :param header_included: If the first row of data is the header of the data
        :param encoding: encoding type the csv is in Latin-1, UTF-8, etc..
        """
        if self.instance_created:
            self._reset()

        self.instance_created = True

        #  Read the csv and send the contents to insert_into_table for processing
        with open(file_name, 'r', encoding=encoding) as infile:
            csv_reader = csv.reader(infile)
            self.insert_data = [r for r in csv_reader]
        self.insert_into_table(table, self.insert_data, overwrite=overwrite, header_included=header_included)

    def insert_csv_into_table(self, table, file_name, overwrite=False):
        """
        Insert a prepared csv into a table
        :param table: The name of the table to insert the data into
        :param file_name: The file path including the file
        :param overwrite: Delete the existing data in the table
        """
        self.overwrite = overwrite
        self.table_name = table

        if overwrite:
            #  Clear Table is needed
            self._clear_table()
        #  Stage CSV and copy into table
        self.cursor.copy_from(file_name, self.table_name, sep=',', null='')

    def create_table(self, table_name, columns, auto_increment_column=0, increment_start_at=1, increment_by=1):
        """
        Create new table
        :param table_name: The name of the new table to create
        :param columns: A list of string containing the Column Name and Type
        :param auto_increment_column: The Column number to auto increment
        :param increment_start_at: The auto increment to start at Default = 1
        :param increment_by: The amount to increment by Default = 1
        """
        self.table_name = table_name
        #  Setup Auto Increment column statement
        if auto_increment_column:
            columns[auto_increment_column - 1] = list(columns[auto_increment_column - 1])
            columns[auto_increment_column - 1][1] = columns[auto_increment_column - 1][1] \
                                                    + ' AUTOINCREMENT START {increment_start_at}, ' \
                                                      'INCREMENT {increment_by}' \
                                                        .format(increment_start_at=increment_start_at,
                                                                increment_by=increment_by)
        #  Assemble the rest of the command
        command = 'CREATE OR REPLACE TABLE {table} ('.format(table=self.table_name)
        column_string = ', '.join([' '.join(set) for set in columns])
        query = command + column_string + ')'
        #  Execute the command
        self.execute_sql_command(query)

    def create_temp_table(self, table_name, columns, auto_increment_column=0, increment_start_at=1, increment_by=1):
        """
        Create new table and drop the table when the connection is closed
        :param table_name: The name of the new table to create
        :param columns: A list of string containing the Column Name and Type
        :param auto_increment_column: The Column number to auto increment
        :param increment_start_at: The auto increment to start at Default = 1
        :param increment_by: The amount to increment by Default = 1
        """
        self.table_name = table_name
        #  Setup Auto Increment column statement
        if auto_increment_column:
            columns[auto_increment_column - 1] = list(columns[auto_increment_column - 1])
            columns[auto_increment_column - 1][1] = columns[auto_increment_column - 1][1] \
                                                    + ' AUTOINCREMENT START {increment_start_at}, ' \
                                                      'INCREMENT {increment_by}' \
                                                        .format(increment_start_at=increment_start_at,
                                                                increment_by=increment_by)
        #  Assemble the rest of the command
        command = 'CREATE OR REPLACE TEMPORARY TABLE {table} ('.format(table=self.table_name)
        column_string = ', '.join([''.join(set) for set in columns])
        query = command + column_string + ')'
        #  Execute the command
        self.execute_sql_command(query)

    def drop_table(self, table):
        """
        Drop a table
        :param table: Name of the table to drop
        """
        self.table_name = table
        #  Create and execute command
        command = 'DROP TABLE {table}'.format(table=self.table_name)
        self.execute_sql_command(command)
