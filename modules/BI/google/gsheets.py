import string
import tempfile

import pygsheets

from . import *


# %% number to letter
def number_to_column_letter(n):
    """
    Converts a number to a letter value
    If the number is greater than 26 it will increment to AA, AB, etc
    :param n: The number to convert to letter
    :return: String of Letter Value
    """
    div = n
    string = ""
    while div > 0:
        mod = (div - 1) % 26
        string = chr(65 + mod) + string
        div = int((div - mod) / 26)
    return string


# %% range_builder
def range_builder(start_row, start_col, end_row=None, end_col=None):
    """
    Builds A1 Notation using the number values
    Start Row and Column are required
    End Row and Column are optional
    start_row 1 + start_col 1 = A1
    start_row 1 + start_col 1 + end_col 2 = A1:B
    start_row 1 + start_col 1 + end_row 2 + end_col 2 = A1:B2
    :param start_row: Starting Row Number
    :param start_col: Starting Column Number
    :param end_row: Ending Row Number (Cannot be used alone like end_col)
    :param end_col: Ending Column Number
    :return: A1 Notation String
    """
    # Uses colnum_string to return a letter value
    start_col_letter = number_to_column_letter(start_col)

    # If both End row and col are not None return Start Cell and Cell "A1:B2"
    if end_row is not None and end_col is not None:
        end_col_letter = number_to_column_letter(end_col)
        range_name = str(start_col_letter) + str(start_row) + ':' + str(end_col_letter) + str(end_row)
    # If only End Col is not None return Start Cell and End Col "A1:B"
    elif end_row is None and end_col is not None:
        end_col_letter = number_to_column_letter(end_col)
        range_name = str(start_col_letter) + str(start_row) + ':' + str(end_col_letter)
    # If both end row and col are None return Single Cell Notation "A1"
    else:
        range_name = str(start_col_letter) + str(start_row)
    return range_name


# %% Google Sheets
class GSheets:
    # named tuple for storing dimensions
    Dimensions = collections.namedtuple('Dimensions', 'rows cols')
    RangeData = collections.namedtuple('RangeData', 'start_row start_col end_row end_col')

    def __init__(self, sheet_id, client_secret=None):
        """
        A class for accessing and modifying a Google Sheet
        :param sheet_id: The URL or key to the Google Sheet
        :param client_secret: File path to existing client secret file
        """
        self.sheet_id = sheet_id
        self._key_extractor()
        self.sheet_name = None
        self.sheet_dimensions = None
        self.data_dimensions = None
        self.existing_data_dimensions = None
        self.results = None
        self._append = False

        self.start_cell = 'A2'
        self.end_cell = None

        self.range_data = None

        self.temp = None
        try:
            #  Create a Temporary Auth file
            #  Create Auth object
            #  Open Google Sheet via ID
            if client_secret:
                self.temp = client_secret
            else:
                self._temp_creds_file()
            self.gc = pygsheets.authorize(outh_file=self.temp.name, outh_creds_store='global', **{'retries': 3})
            self.spreadsheet = self.gc.open_by_key(self.sheet_id)
        except BaseException as e:
            raise e
        finally:
            #  Delete Temp File
            if not client_secret:
                os.remove(self.temp.name)

    def _temp_creds_file(self):
        """
        Creates a temporary file to use when authorizing
        Client Secret is stored in System Environment Variable
        """
        #  Extract system variable and open as json
        service_file_json = json.loads(os.environ.get('GOOGLE_SECRET_CREDS'))
        #  Create temp file, open file, write json, and close file
        self.temp = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.temp.write(json.dumps(service_file_json))
        self.temp.close()

    def _key_extractor(self):
        """
        Extract the key from a url for the many Google Doc types
        such as a Google Sheet or a Google Drive File or Folder
        Example URL: https://docs.google.com/spreadsheets/d/<spreadsheet key>/edit#gid=0
        It evaluates what type of URL it has been handed and then extracts the key
        :return: sets self.sheet_id to the key
        """
        drive_key_len = 28
        url = self.sheet_id
        if url is not None:
            if len(url) > drive_key_len:

                if 'file/d/' in url:
                    file_start = url.index('file/d/') + len('file/d/')
                    new_url = url[file_start:]
                    self.sheet_id = new_url[:new_url.index('/')]

                elif 'spreadsheets/d/' in url:
                    file_start = url.index('spreadsheets/d/') + len('spreadsheets/d/')
                    new_url = url[file_start:]
                    self.sheet_id = new_url
                    if '/' in new_url:
                        new_url = new_url[:new_url.index('/')]
                        self.sheet_id = new_url

                elif 'folders/' in url:
                    file_start = url.index('folders/') + len('folders/')
                    self.sheet_id = url[file_start:]

                elif 'id=' in url:
                    file_start = url.index('id=') + len('id=')
                    self.sheet_id = url[file_start:]

    def _number_to_column_letter(self, n):
        """
        Converts a number to a letter value
        If the number is greater than 26 it will increment to AA, AB, etc
        :param n: The number to convert to letter
        :return: String of Letter Value
        """
        div = n
        string = ""
        while div > 0:
            mod = (div - 1) % 26
            string = chr(65 + mod) + string
            div = int((div - mod) / 26)
        return string

    def _column_letter_to_number(self, letter):
        num = 0
        for c in letter:
            if c in string.ascii_letters:
                num = num * 26 + (ord(c.upper()) - ord('A')) + 1
        return num

    def set_active_sheet(self, sheet_name):
        if sheet_name:
            try:
                self.sheet_name = sheet_name
                self._get_dimensions()
            except pygsheets.WorksheetNotFound as e:
                raise e

    def sheet1(self):
        self.sheet_name = 'Sheet1'
        self._get_dimensions()

    def _get_dimensions(self, rows=None, columns=None):
        if not rows:
            rows = self.get_row_count()
        if not columns:
            columns = self.get_column_count()
        self.sheet_dimensions = self.Dimensions(rows, columns)

    def get_row_count(self):
        """
        Get the last row with data in the sheet
        :return: The last row number with data in it
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        return wks.rows

    def get_column_count(self):
        """
        Get the last column with data in the sheet
        :return: The last column number with data in it
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        return wks.cols

    def add_rows(self, number_of_rows):
        """
        Add rows to a desired sheet
        :param number_of_rows: The number of rows to add
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        wks.add_rows(number_of_rows)
        self._get_dimensions()

    def add_cols(self, number_of_columns):
        """
        Add columns to a desired sheet
        :param number_of_columns: The number of columns to add
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        wks.add_cols(number_of_columns)
        self._get_dimensions()

    def resize_sheet(self, rows, columns):
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        wks.resize(rows=rows, cols=columns)
        self._get_dimensions(rows=rows, columns=columns)

    def get_sheet_data(self):
        """
        Grabs all of the data from an entire sheet as a list of lists
        :return: The List of Lists containing the sheets data
        """
        self.sheet_name = self.sheet_name
        try:
            wks = self.spreadsheet.worksheet('title', self.sheet_name)
            self.results = wks.get_all_values()
        except pygsheets.WorksheetNotFound as e:
            raise e
        except Exception as e:
            raise e

    def clear_sheet_data(self):
        """
        Clears the all worksheet values by default
        If start_cell and end_cell have been set, clears specified range
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        wks.clear(start=self.start_cell, end=self.end_cell)

    def _find_existing_data_bounding_box(self):
        self.get_sheet_data()
        last_row = len(self.results)
        last_col = max(len(x) for x in self.results)
        self.existing_data_dimensions = self.Dimensions(last_row, last_col)

    def _create_range_data(self):
        start_row = 1
        start_col = 2
        if self.start_cell != 'A2':
            start_row = int(re.findall('\d+', self.start_cell)[0])
            start_col = self._column_letter_to_number(' '.join(re.findall("[a-zA-Z]+", self.start_cell)))
        end_row = 0
        end_col = 0
        if self.end_cell:
            end_row = int(re.findall('\d+', self.end_cell)[0])
            end_col = self._column_letter_to_number(' '.join(re.findall("[a-zA-Z]+", self.end_cell)))

        self.range_data = self.RangeData(start_row, start_col, end_row, end_col)

    def _create_start_end_cell(self):
        self.start_cell = range_builder(self.range_data.start_row, self.range_data.start_col)
        if self.range_data.end_row and self.range_data.end_col:
            self.end_cell = range_builder(self.range_data.end_row, self.range_data.end_col)
        elif self.range_data.end_row and not self.range_data.end_col:
            self.end_cell = range_builder(self.range_data.end_row,
                                          self.sheet_dimensions.cols - self.range_data.start_col)
        elif not self.range_data.end_row and self.range_data.end_col:
            self.end_cell = range_builder(self.sheet_dimensions.rows - self.range_data.start_row,
                                          self.range_data.end_col)

    def _adjust_sheet_for_data(self):
        #  Check for number of new rows or cols needed
        self._create_start_end_cell()
        new_rows = (self.data_dimensions.rows - self.sheet_dimensions.rows) + (self.range_data.start_row - 1)
        new_cols = (self.data_dimensions.cols - self.sheet_dimensions.cols) + (self.range_data.start_col - 1)
        if self._append:
            self._find_existing_data_bounding_box()
            if self.existing_data_dimensions.rows > self.range_data.start_row:
                self.range_data = self.range_data._replace(start_row=self.existing_data_dimensions.rows + 1)
            empty_rows = self.sheet_dimensions.rows - self.existing_data_dimensions.rows
            new_rows = (self.data_dimensions.rows - empty_rows)
            self.start_cell = range_builder(self.range_data.start_row, self.range_data.start_col)
        else:
            self.clear_sheet_data()

        if new_rows > 0:
            self.add_rows(new_rows)
        #  Add additional columns if there are not enough to paste the data
        if new_cols > 0:
            self.add_cols(new_cols)

    def update_sheet(self, data_for_upload, range_data=None, start_cell='A2', end_cell=None, append=False):
        """
        Pushes Data to the active sheet
        :param data_for_upload: (Required) A List of Lists or Pandas DataFrame containing the new values
        :param range_data: A namedtuple with start_row, start_col, end_row, end_col as integers
                            Used instead or start_cell and end_cell
        :param start_cell: Default 'A2'
                            Top left cell address ('A2')
        :param end_cell: (Optional) Bottom right cell address ('B2')
                            If not provided automatically determined to match size of data
        :param append: Default False
                        If append is true append data to sheet else clear existing data
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        self._append = append

        if range_data:
            self.range_data = range_data
        if start_cell != 'A2':
            self.start_cell = start_cell
            self._create_range_data()
        elif end_cell:
            self.end_cell = end_cell
            self._create_range_data()

        #  If data_for_upload is not a Data Frame, convert it to Data Frame
        if not isinstance(data_for_upload, pd.DataFrame):
            df = pd.DataFrame(data_for_upload)
        else:
            df = data_for_upload

        #  set dimensions of the Data Frame
        self.data_dimensions = self.Dimensions._make(df.shape)

        #  Modify sheet to for new data
        self._adjust_sheet_for_data()

        #  Push the data to the sheet
        wks.set_dataframe(df, self.start_cell, copy_head=False, nan='')

    def update_named_range(self, range_name, value):
        """
        Update a named range with a new value
        :param range_name: Name of the named range
        :param value: New value to insert
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name).update_cell(range_name, value)

    def get_named_range(self, range_name):
        """
        Return the value of the named range
        :param range_name: Name of named range
        :return: Value in named range
        """
        wks = self.spreadsheet.worksheet('title', self.sheet_name)
        range_data = wks.get_named_range(range_name)
        self.results = range_data
