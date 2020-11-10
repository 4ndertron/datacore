import pandas as pd
from modules import json
from modules.little_gsheet import GSheet
from modules.module_enums import SQLText
from modules.data_handler import DataHandler
from modules.module_enums import JobNimbus_to_WPEngine_Mapping as Mapping


def dh_main():
    creds_string = open('./secrets/creds.json', 'r').read()
    creds = json.loads(creds_string)
    data_handler = DataHandler(creds=creds['databases'])
    return data_handler


def zipper(**kwargs):
    """
    This function will take the raw job nimbus data, extract the columns required for the wp engine, and rename
    the columns to the pivot table column names in WPEngine.

    """
    jn = kwargs.get('jn')
    mapper = kwargs.get('map')
    bridge = kwargs.get('bridge')
    pm = mapper['system']['post_meta']
    new_cols = {v[0]: k for k, v in pm.items()}
    new_jn = jn.loc[:, [x for x in new_cols.keys()]]
    new_jn.rename(columns=new_cols, inplace=True)
    # create custom org by iterating through the columns
    # parse new_jn into new org
    # append parsed df to their corresponding pivot table.
    # todo: Find a way to tie an account's post ID to the account_id so the parsed DF's can attach the required post_id
    return [pm, new_cols, new_jn]


def main():
    return 0


def audit():
    # What to lookout for...
    #   1. address validation
    #   2. generic customer data
    #   3. missing data
    #   4. easily crackable passwords
    #   5. escaped tab characters in text fields (found in usermeta).
    return 0


if __name__ == '__main__':
    m = main()
    dh = dh_main()

    update_returns = dh.update_local_wp(['wp_users', 'wp_usermeta', 'wp_posts', 'wp_postmeta'])

    pitt = dh.engines['pitt_engine']
    loc = dh.engines['docker_engine']
    lite = dh.engines['sqlite_engine']

    pivot_tables = ['wp_commentmeta', 'wp_postmeta', 'wp_termmeta', 'wp_usermeta']
    melt_schemas = ['wp_postmeta_pivot', 'wp_usermeta_pivot']
    jn_map = Mapping.conversion_map.value

    # jn_df = pd.read_sql("select * from jobnimbus.contact", loc.engine)
    # dft = jn_df.assign(account_id=lambda df: df.loc[:, 'Address Line']
    #                                          + ', ' + df.loc[:, 'City']
    #                                          + ', ' + df.loc[:, 'State']
    #                                          + ', ' + df.loc[:, 'Zip']
    #                                          + ', USA')

    # for i in range(3):  # This works
    #     account_post = dh.create_single_post_df(post_type='account', creator_id=5, source_engine=loc)
    #     account_post.to_sql('wp_posts', loc.engine, schema='wp_liftenergypitt', if_exists='append', index=False)

    convert_returns = dh.convert_jn_tables_to_wp(jn_engine=loc, tp_engine=pitt, ld_engine=loc, field_map=jn_map)
    bridge = convert_returns[3]
    users_dict = convert_returns[4]

    gs = GSheet('1JH7x89nosp4gCJLAWZ9CkzJroxBnhMhIMqGTF7I6jm0')
