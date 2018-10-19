from parsl.app.errors import ParslError


class OptionalModuleMissing(ParslError):
    ''' Error raised when a required module is missing for a optional/extra component
    '''

    def __init__(self, module_names, reason):
        self.module_names = module_names
        self.reason = reason

    def __repr__(self):
        return "The functionality requested requires a missing optional module:{0},  Reason:{1}".format(
            self.module_names, self.reason
        )
