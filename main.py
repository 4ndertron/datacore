from modules import os
from modules import json
from modules import secrets_dir
from modules.auditor import Auditor


def load_custom_credentials():
    with open(os.path.join(secrets_dir, 'custom_credentials.json'), 'r') as cc:
        jt = json.loads(cc.read())
        return jt  # specific durations wanted to look into.


if __name__ == '__main__':
    my_credentials = load_custom_credentials()
    auditor = Auditor(credentials=my_credentials)

    dh = auditor.data_handler
    api = auditor.api_handler

    pitt = auditor.default_pitt_engine
    loc = auditor.default_loc_engine
    lite = auditor.default_lite_engine
    gm = api.apis['google_maps_place']

    ra = auditor.run_auditor()
