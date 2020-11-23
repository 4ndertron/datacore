import modules as pm  # pm being an acronym for project_modules
from .core_units.rest_api_core import RestCore


class ApiHandler:
    """
    The content for this class was inspired by the google maps places api located at the following url:
    https://developers.google.com/places/web-service/search?hl=en_US#TextSearchRequests
    """

    def __init__(self, **kwargs):
        pm.logging.debug(f'running init of class ApiHandler in {__name__}')
        self.init_kwargs = kwargs
        self.inti_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.apis = {}
        self.credentials = kwargs.get('credentials')
        self._setup_apis()

    def _setup_apis(self):
        pm.logging.debug(f'running _setup_apis in ApiHandler')
        for api, api_cred in self.credentials.items():
            pm.logging.debug(f'api: {api}, has a cred of {api_cred}')
            self.apis[api] = RestCore(credentials=api_cred)

    def update_apis(self, credentials):  # fix
        pm.logging.debug(f'running update_apis in ApiHandler')
        returns = {}
        for api, cred in credentials.items():
            if api in self.apis:
                api_update_returns = self.apis[api].update_api(cred)
                returns['replace'] = api_update_returns
            else:
                self.apis[api] = RestCore(credentials=cred)
                returns['append'][api] = cred
        return returns

    def return_search(self, search_text, **kwargs):
        pm.logging.debug(f'running return_search in GMapPlace')
        rest_core = kwargs.get('rest_core')
        if not rest_core:  # establish the handler's first api as the default rest_core to use.
            pm.logging.debug(f'No rest_core parameter was passed. Using first listed api in handler.')
            rest_core = self.apis[[x for x in self.apis.keys()][0]]
        search_url = rest_core.api_url
        payload = {
            'query': search_text,
            'key': rest_core.api_payload['key'],
        }
        pm.logging.debug(f'search_url during function call {__name__}: {search_url}')
        r = pm.rq.get(search_url, params=payload)
        return {search_url: payload, 'response': r}
