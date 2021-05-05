from parsl.app.errors import ParslError


class OptionalModuleMissing(ParslError):
    ''' Error raised when a required module is missing for a optional/extra component
    '''

    def __init__(self, module_names, reason):
        self.module_names = module_names
        self.reason = reason

    def __repr__(self):
        msg = (f"The functionality requested requires a missing optional module"
               f":{self.module_names},  Reason:{self.reason}")
        return msg
