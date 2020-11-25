import modules as pm
from .core_units.gs_core import GSheet


class GSheetHandler:
    def __init__(self, **kwargs):
        pm.logging.debug(f'running init of class ApiHandler in {__name__}')
        self.init_kwargs = kwargs
        self.inti_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.gsd = {}
        self.credentials = kwargs.get('credentials')
        self._setup_gs()

    def _setup_gs(self):
        pm.logging.debug(f'running _setup_gs in GSheetHandler')
        for gs, gs_id in self.credentials.items():
            pm.logging.debug(f'sheet {gs} has id: {gs_id}')
            self.gsd[gs] = GSheet(gs_id)

    def update_gs(self, credentials):
        pm.logging.debug(f'running update_gs in GSheetHandler')
        returns = {}
        for gs, gs_id in credentials.items():
            if gs in self.gsd:
                returns['update'] = gs
            else:
                returns['create'] = gs
            self.gsd = GSheet(gs_id)
        return returns

    def update_range(self, **kwargs):
        target_range = kwargs.get('range')
        data = kwargs.get('data')
        sheet = kwargs.get('sheet')
        if type(data) in ('df', 'series'):
            data = data.to_list()
        r = sheet.update_range(target_range, data)
        return {
            'range': target_range,
            'data_cols': len(data[0]),
            'data_rows': len(data),
            'sheet': sheet,
            'r': r,
        }
