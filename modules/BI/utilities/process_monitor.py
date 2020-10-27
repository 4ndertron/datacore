from . import *
import win32pdh
import signal
import time
from BI.data_warehouse import SnowflakeConnectionHandlerV2, SnowflakeV2
from subprocess import Popen, CREATE_NEW_CONSOLE


def get_process_id(name):
    object = "Process"
    items, instances = win32pdh.EnumObjectItems(None, None, object,
                                                win32pdh.PERF_DETAIL_WIZARD)
    val = None
    if name in instances:
        hq = win32pdh.OpenQuery()
        hcs = []
        item = "ID Process"
        path = win32pdh.MakeCounterPath((None, object, name, None, 0, item))
        hcs.append(win32pdh.AddCounter(hq, path))
        win32pdh.CollectQueryData(hq)
        time.sleep(0.01)
        win32pdh.CollectQueryData(hq)

        for hc in hcs:
            type, val = win32pdh.GetFormattedCounterValue(hc, win32pdh.PDH_FMT_LONG)
            win32pdh.RemoveCounter(hc)
        win32pdh.CloseQuery(hq)
        return val


class ProcessMonitor:
    def __init__(self, process_name):
        self.name = process_name
        self.pid = self.get_pid()

    def get_pid(self):
        return get_process_id(self.name)

    def kill_process(self):
        os.popen('TASKKILL /PID ' + str(self.pid) + ' /F')

    def restart_process(self, bat_file):
        Popen(bat_file, creationflags=CREATE_NEW_CONSOLE)


class UrlMonitor(ProcessMonitor):
    def __init__(self, process_name, url, bat_file):
        super().__init__(process_name)
        self.url = url
        self.file_name = bat_file
        self.responding = False

    def _url_is_responding(self):
        status_code = None
        try:
            results = requests.get(self.url, timeout=10)
            status_code = results.status_code
        except requests.exceptions.ConnectionError:
            status_code = "Connection refused"
        if status_code == requests.codes.ok:
            self.responding = True

    def run(self):
        self._url_is_responding()
        if not self.responding:
            if self.pid:
                self.kill_process()
            self.restart_process(self.file_name)


class AutomatorMonitor(ProcessMonitor):
    def __init__(self, process_name, bat_file):
        super().__init__(process_name)
        self.file_name = bat_file
        self.meta_data = None
        self._get_meta_data()

    def _get_meta_data(self):
        dw = SnowflakeV2(SnowflakeConnectionHandlerV2())
        dw.get_table_data('D_POST_INSTALL.T_AUTO_META_DATA')
        MetaData = collections.namedtuple('MetaData', ' '.join(name.lower() for name in dw.column_names))
        self.meta_data = MetaData._make(dw.query_results[0])

    def _check_last_run(self):
        if self.meta_data.last_run:
            hours_passed = (dt.datetime.now() - self.meta_data.last_run).seconds / 60 / 60
            if hours_passed > 1:
                if self.pid:
                    self.kill_process()
                self.restart_process(self.file_name)
