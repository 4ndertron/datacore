import pandas as pd
from modules.data_handler import DataHandler
from modules.module_enums import JobNimbus_to_WPEngine_Mapping as Mapping
from modules.module_enums import SQLText
from modules import json


def dh_main():
    creds_string = open('./secrets/creds.json', 'r').read()
    creds = json.loads(creds_string)
    data_handler = DataHandler(creds=creds)
    return data_handler


def main():
    return 0


if __name__ == '__main__':
    m = main()
    dh = dh_main()

    pitt = dh.engines['pitt_engine']
    loc = dh.engines['docker_engine']
    lite = dh.engines['sqlite_engine']

    pivot_tables = ['wp_commentmeta', 'wp_postmeta', 'wp_termmeta', 'wp_usermeta']
    melt_schemas = ['wp_postmeta_pivot', 'wp_usermeta_pivot']
    jn_map = Mapping.conversion_map.value

    convert_returns = dh.convert_jn_tables_to_wp(jn_engine=loc, field_map=jn_map)
    bridge = convert_returns[2]
    bridge_dict = bridge.to_dict()
    jn_df = pd.read_sql("select * from jobnimbus.contact", loc.engine)
    dft = jn_df.assign(account_id=lambda df: df.loc[:, 'Address Line']
                                             + ', ' + df.loc[:, 'City']
                                             + ', ' + df.loc[:, 'State']
                                             + ', USA')
    # for i in range(3):  # This works
    #     account_post = dh.create_single_post_df(post_type='account', creator_id=5, source_engine=loc)
    #     account_post.to_sql('wp_posts', loc.engine, schema='wp_liftenergypitt', if_exists='append', index=False)
