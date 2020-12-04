import modules as pm
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


class GSheet:
    def __init__(self, sheet_id, **kwargs):
        pm.logging.debug(f'running init of class GMapPlace in {__name__}')
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.init_kwargs = kwargs
        self.init_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.creds = None
        self.service = None
        self.sheet_id = sheet_id
        self.pickle_path = pm.os.path.join(pm.secrets_dir, 'token.pickle')
        self.cred_path = pm.os.path.join(pm.secrets_dir, 'client_secret.json')
        self._set_service()
        self.sheet = self.service.spreadsheets()

    def _set_service(self):
        # copied from the google development api example.
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        pm.logging.debug(f'running _set_service in GSheet')
        if pm.os.path.exists(self.pickle_path):
            with open(self.pickle_path, 'rb') as token:
                self.creds = pm.pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.cred_path, self.SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.pickle_path, 'wb') as token:
                pm.pickle.dump(self.creds, token)

        self.service = build('sheets', 'v4', credentials=self.creds)

    def gather_column(self, sheet_range, val_col_int=0):
        """
        This method will collect all the values in a range on the spreadsheet, and return all the values in a field
        within the range.
        :param sheet_range: Google Range syntax of the range you want to search.
        :param val_col_int: The column number with the values you want returned
        :return: a list containing all the values in the specified column within the range.
        """
        pm.logging.debug(f'running gather_column in GSheet')
        result = self.sheet.values().get(spreadsheetId=self.sheet_id,
                                         range=sheet_range).execute()
        values = result.get('values', [])
        account_list = []
        for row in values:
            account_list.append(row[val_col_int])
        return account_list

    def gather_range_values(self, sheet_range):
        """
        This method will collect the values of a given range in the google sheet id provided at the creation of the obj.
        :param sheet_range: The range in the sheet you want the values from.
        :return: a list of list of the values in the range.
        """
        pm.logging.debug(f'running gather_range_values in GSheet')
        result = self.sheet.values().get(
            spreadsheetId=self.sheet_id,
            range=sheet_range
        ).execute()
        values = result.get('values', [])
        if not values:
            return 'No data found'
        else:
            return values

    def append_range(self, sheet_range, list_of_list_data):
        """
        This method is meant to append data to a range within a google sheet.
        :param sheet_range: Google Range syntax of the range you want to update.
        :param list_of_list_data: a list of list data set that you want added to the bottom of the range.
        :return: the api response after executing the request.
        """
        pm.logging.debug(f'running append_range in GSheet')
        new_records = {
            'majorDimension': 'ROWS',
            'values': list_of_list_data
        }
        request = self.sheet.values().append(
            spreadsheetId=self.sheet_id,
            range=sheet_range,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=new_records
        )
        response = request.execute()
        return response

    def update_range(self, sheet_range, list_of_list_data):
        """
        This method is meant to update the values in a specific range.
        :param sheet_range: Google Range syntax of the range you want to update.
        :param list_of_list_data: a list of list data set that is the size of the sheet range.
        :return: the api response after executing the request.
        """
        pm.logging.debug(f'running update_range in GSheet')
        new_records = {
            # 'majorDimensions:': 'ROWS',
            'values': list_of_list_data
        }
        request = self.sheet.values().update(
            spreadsheetId=self.sheet_id,
            range=sheet_range,
            valueInputOption='USER_ENTERED',
            body=new_records
        )
        response = request.execute()
        return response
