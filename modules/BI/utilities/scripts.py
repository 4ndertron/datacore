import subprocess as sp

from . import *


# %% Python Script
class Script:
    """
    PythonScript used to run additional python scripts
    """

    def __init__(self, file_path):
        """
        :param file_path: file_path including .py file
        """
        self.script_path = file_path
        self.script_dir = os.path.dirname(self.script_path)
        self.script_name = os.path.basename(self.script_path)

        self.communicate = None
        self.stream_data = None

        self.rc = None

    def run_script(self):
        """
        Call python script and read Return Codes
        """
        try:
            # Generate command console python command
            command = 'python' + ' "' + self.script_path + '"'
            current_dir = os.getcwd()
            os.chdir(self.script_dir)
            child = sp.Popen(command)
            self.communicate = child.communicate()
            self.stream_data = child.communicate()[0]

            # rc is the return code of the script
            self.rc = child.returncode
            os.chdir(current_dir)

        except Exception as e:
            raise e
