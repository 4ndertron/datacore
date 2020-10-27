from urllib.parse import quote

from . import *


# %% InContact Reporting

class InContactReport:
    def __init__(self, cdst_token, preset_date=0, date_range=None, include_header=True):
        """
        Use to download any InContact Data Download report
        :param cdst_token: The CURL token for authentifying the request
        :param preset_date: Default = 2
                            1 = Today
                            2 = Yesterday
                            3 = Last 7 Days
                            4 = Last 30 Days
                            5 = Previous Week
                            6 = Previous Month
                            7 = Month to Date
        :param date_range:  A tuple or list containing the Start Date and End_Date in that order.  Will be used
        if included and ignore preset_date
        :param include_header: If the 1st row should contain the column headers
        """
        self.base_url = 'https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?'
        self.cdst = cdst_token
        self.include_header = include_header
        #  No file is downloaded so append_data and format will not do anything but are required for the request
        self.append_date = True
        self.format = 'CSV'

        #  If date range is included, unpack date and time and assign to variables
        #  Else use preset_date or default
        if date_range is not None:
            self.preset_date = 0
            self.preset_date_range = date_range

            self.start_date = date_range[0].strftime("%m/%d/%Y %I:%M %p")
            self.start_date, self.start_time, self.start_time_ampm = tuple(self.start_date.split(' '))

            self.end_date = date_range[1].strftime("%m/%d/%Y %I:%M %p")
            self.end_date, self.end_time, self.end_time_ampm = tuple(self.end_date.split(' '))
        else:
            if not preset_date:
                self.preset_date = 2
            else:
                self.preset_date = preset_date

        self.url = ''

        self.report_header = []
        self.report_results = []
        self.get_report()

    def create_url(self):
        """
        Create the url for request
        :return:
        """
        self.url = self.base_url + 'CDST=' + self.cdst
        #  Base url combine with date string and formatting
        if self.preset_date:
            date_str = "&presetDate=" + str(self.preset_date)
        else:
            date_str = "&DateFrom=" + quote(self.start_date, safe='') \
                       + "+" + quote(self.start_time, safe='') \
                       + '+' + self.start_time_ampm \
                       + "&DateTo=" + quote(self.end_date, safe='') \
                       + '+' + quote(self.end_time, safe='') \
                       + '+' + self.end_time_ampm

        self.url = self.url \
                   + date_str \
                   + "&Format=" + self.format \
                   + "&IncludeHeaders=" + str(self.include_header) \
                   + "&AppendDate=" + str(self.append_date)

    def get_report(self):
        #  Create Url
        self.create_url()
        #  Start session
        with requests.Session() as s:
            #  Get the report
            download = s.get(self.url)
            #  Unpack/Decode the report
            decoded_content = download.content.decode('utf-8')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            self.report_results = list(cr)
            #  Remove header from data if include and store seperately
            if self.include_header:
                self.report_header = self.report_results[0]
                del self.report_results[0]
            #  End session
            s.close()
