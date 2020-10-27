from . import *

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def find_main_dir():
    """
    Returns the Directory of the main running file
    """
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)

    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(os.path.realpath(__file__))


class CrawlerBase:
    """
    CrawlerBase is a web crawler, that will login to a website provide the Login link
    The Username, Password, and submit button html ID's and the values to be entered
    """
    chrome_driver = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\chromedriver.exe'
    firefox_driver = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\geckodriver.exe'
    delay = 10

    def __init__(self, driver_type, download_directory=None, headless=False):
        self.skip = False
        self.headless = headless
        self.driver_type = driver_type.lower()
        self.download_directory = download_directory
        self.console_output = False
        self.driver = None
        self.options = None
        self.active = False

    def enable_console_output(self):
        self.console_output = True

    def _start_driver(self):
        if self.driver_type == 'chrome':
            self.create_chrome_driver()
        elif self.driver_type == 'firefox':
            self.create_firefox_driver()

    def create_firefox_driver(self):
        fp = webdriver.FirefoxProfile()
        self.options = webdriver.FirefoxOptions()

        # Default Preference Change stops Fingerprinting on site
        fp.DEFAULT_PREFERENCES['frozen']['dom.disable_open_during_load'] = True

        # Set the Download preferences to change the download folder location and to auto download reports
        fp.set_preference("browser.download.panel.shown", False)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk",
                          'application/csv, '
                          'application/octet-stream, '
                          'text/csv, '
                          'application/vnd.ms-excel, '
                          'text/comma-separated-values, '
                          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        fp.set_preference('browser.download.folderList', 2)
        fp.set_preference("browser.helperApps.alwaysAsk.force", False)
        fp.set_preference("browser.download.manager.alertOnEXEOpen", False)
        fp.set_preference("browser.download.manager.closeWhenDone", True)
        fp.set_preference("browser.download.manager.showAlertOnComplete", False)
        fp.set_preference("browser.download.manager.useWindow", False)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        if self.download_directory:
            fp.set_preference('browser.download.dir', self.download_directory)
        self.driver = webdriver.Firefox(executable_path=self.firefox_driver, options=self.options, firefox_profile=fp)

        if not self.headless:
            # Set driver to headless when not in testing mode
            self.options.add_argument('--headless')

    def create_chrome_driver(self):
        self.options = webdriver.ChromeOptions()

        preferences = {
            'profile.default_content_settings.popups': 0,
            'download.prompt_for_download': False
        }
        if self.download_directory:
            preferences['download.default_directory'] = self.download_directory + '\\'
            preferences['download.directory_upgrade'] = True

        self.options.add_experimental_option('prefs', preferences)
        self.options.add_argument("disable-infobars")

        if self.headless:
            self.options.add_argument('headless')

        self.driver = webdriver.Chrome(executable_path=self.chrome_driver, options=self.options)

    def delete_cookies(self):
        # Create the Driver and Delete all cookies
        self.driver.delete_all_cookies()

    def login(self, **kwargs):
        # Use Driver to login
        # Go to Login Page
        if self.console_output:
            print('Logging into {url}'.format(url=kwargs.get('url')))
        self._start_driver()
        self.driver.get(kwargs.get('url'))
        self._input(wait=True, **kwargs.get('username'))
        self._input(wait=True, **kwargs.get('password'))
        self._input(click=True, **kwargs.get('submit'))
        self.active = True

    def _input(self, wait=False, click=False, **kwargs):
        if 'id' in kwargs.keys():
            if wait:
                WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((By.ID,
                                                                                             kwargs.get('id'))))
            input = self.driver.find_element_by_id(kwargs.get('id'))
        elif 'class' in kwargs.keys():
            if wait:
                WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((By.CLASS_NAME,
                                                                                             kwargs.get('class'))))
            input = self.driver.find_element_by_class_name(kwargs.get('class'))
        elif 'name' in kwargs.keys():
            if wait:
                WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((By.NAME,
                                                                                             kwargs.get('name'))))
            input = self.driver.find_element_by_name(kwargs.get('name'))
        if not click:
            input.send_keys(kwargs.get('input'))
        else:
            input.click()

    def end_crawl(self):
        # Close the Driver
        if self.console_output:
            print('Crawler Tasks Complete')
        self.driver.quit()
        self.active = False
