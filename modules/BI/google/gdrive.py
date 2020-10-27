from . import *
from pathlib import Path
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


# %% Google Drive
class GDrive:
    user_dir = Path.home()
    yaml_file = os.path.join(user_dir, '.credentials', 'settings.yaml')

    def __init__(self, credentials=None):
        self.gAuth = GoogleAuth(settings_file=self.yaml_file)
        self.gAuth.LocalWebserverAuth()
        self.drive = GoogleDrive(self.gAuth)

    def key_extractor(self, url):
        drive_key_len = 28
        spreadsheet_key_len = 44
        if url is not None:
            if len(url) > drive_key_len \
                    or len(url) > spreadsheet_key_len:

                if 'file/d/' in url:
                    file_start = url.index('file/d/') + len('file/d/')
                    new_url = url[file_start:]
                    return new_url[:new_url.index('/')]

                if 'spreadsheets/d/' in url:
                    file_start = url.index('spreadsheets/d/') + len('spreadsheets/d/')
                    new_url = url[file_start:]
                    return new_url[:new_url.index('/')]

                if 'folders/' in url:
                    file_start = url.index('folders/') + len('folders/')
                    return url[file_start:]

                if 'id=' in url:
                    file_start = url.index('id=') + len('id=')
                    return url[file_start:]

        return url

    def list_files(self, folder_id):
        folder_id = self.key_extractor(folder_id)
        query = {'q': "'{folder_id}' in parents and trashed=false".format(folder_id=folder_id)}
        return self.drive.ListFile(query).GetList()

    def get_existing_file_id(self, file_name, drive_folder_id):
        drive_folder_id = self.key_extractor(drive_folder_id)
        file_list = self.list_files(drive_folder_id)
        for file in file_list:
            if file['title'] == file_name:
                return file.get('id')
        return None

    def upload_file(self, file_name, file_path, drive_folder_id='root', replace_existing=False):
        file = os.path.join(file_path, file_name)
        if drive_folder_id != 'root':
            drive_folder_id = self.key_extractor(drive_folder_id)
        upload_file = self.drive.CreateFile(metadata={
            'title': file_name,
            "parents": [
                {
                    "kind": "drive#fileLink",
                    "id": drive_folder_id
                }
            ]
        })
        if replace_existing:
            drive_file_id = self.get_existing_file_id(file_name, drive_folder_id)
            if drive_file_id:
                upload_file['id'] = drive_file_id

        upload_file.SetContentFile(file)
        upload_file.Upload()

    def download_file(self, drive_id, file_name, file_location=None):
        drive_id = self.key_extractor(drive_id)
        f = self.drive.CreateFile({'id': drive_id})
        mime_type = f['mimeType']
        if file_location:
            f.GetContentFile(os.path.join(file_location, file_name), mimetype=mime_type)
        else:
            f.GetContentFile(file_name, mimetype=mime_type)

    def get_file_name(self, file_id):
        file_id = self.key_extractor(file_id)
        return str(self.drive.CreateFile({'id': str(file_id)})['title'])

    def write_drive_file(self, file_name, contents, replace_existing=False, folder_id='root'):
        if folder_id != 'root':
            folder_id = self.key_extractor(folder_id)
        f = self.drive.CreateFile(metadata={
            'title': file_name,
            "parents": [
                {
                    "kind": "drive#fileLink",
                    "id": folder_id
                }
            ]
        })
        if replace_existing:
            existing_id = self.get_existing_file_id(file_name, folder_id)
            if existing_id:
                f['id'] = existing_id

        f.SetContentString(contents)
        f.Upload()

    def read_drive_file(self, file_id):
        file_id = self.key_extractor(file_id)
        f = self.drive.CreateFile({'id': file_id})
        return f.GetContentString()
