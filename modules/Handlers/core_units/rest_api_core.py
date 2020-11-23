import modules as pm  # pm being an acronym for project_modules


class RestCore:
    """
    This class is really just a text managemtn class designed for the simeple reqests.get() method
    and the data structure located in the custom_credentials.json file in the main project directory.
    """

    def __init__(self, **kwargs):
        pm.logging.debug(f'running init of class RestCore in {__name__}')
        self.init_kwargs = kwargs
        self.inti_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.credentials = kwargs.get('credentials')
        self.api_url = '/'.join([x for x in self.credentials.get('url').values()])
        self.api_payload = self.credentials.get('payload')
        self.creation_event = {
            pm.dt.datetime.now().strftime(pm.dt_format): {
                'url': {
                    'old': '',
                    'new': self.api_url
                },
                'payload': {
                    'old': '',
                    'new': self.api_payload
                }
            }
        }
        self.update_log_df = self._convert_event_into_df(self.creation_event)

    def update_api(self, **kwargs):
        pm.logging.debug(f'running update_api in RestCore')
        kwarg_cred = kwargs.get('credentials')
        update_timestamp = pm.dt.datetime.now().strftime(pm.dt_format)
        update_event = {update_timestamp: {'url': {}, 'payload': {}}}
        update_event[update_timestamp]['url']['old'] = self.api_url
        update_event[update_timestamp]['payload']['old'] = self.api_payload
        self.api_url = '/'.join([x for x in kwarg_cred.get('url').values()])
        self.api_payload = kwarg_cred.get('payload')
        update_event[update_timestamp]['url']['new'] = self.api_url
        update_event[update_timestamp]['payload']['new'] = self.api_payload
        update_event_df = self._convert_event_into_df(update_event)
        self.update_log_df = pm.pd.concat([self.update_log_df, update_event_df])
        return update_event

    def _convert_event_into_df(self, event):
        pm.logging.debug(f'running _convert_event_into_df in RestCore')
        event_ts = [x for x in event.keys()]
        column_layer_1 = []
        column_layer_2 = []
        records = [[]]
        for event_data in event.values():
            pm.logging.debug(f'entered the event variable\'s values.')
            for event_type, type_data in event_data.items():
                if event_type not in column_layer_1:
                    pm.logging.debug(f'appending {event_type} to the column_layer_1 list.')
                    column_layer_1.append(event_type)
                for k, v in type_data.items():
                    if k not in column_layer_2:
                        pm.logging.debug(f'appending {k} to the column_layer_2 list.')
                        column_layer_2.append(k)
                    pm.logging.debug(f'appending {v} to the records list.')
                    records[0].append(v)
        column_index = pm.pd.MultiIndex.from_product([column_layer_1, column_layer_2])
        special_df = pm.pd.DataFrame(records, index=event_ts, columns=column_index)
        return special_df
