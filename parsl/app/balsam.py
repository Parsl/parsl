from functools import update_wrapper
from functools import partial
from inspect import signature, Parameter

from parsl.app.errors import wrap_error
from parsl.app.app import AppBase
from parsl.dataflow.dflow import DataFlowKernelLoader

from balsam.site import ApplicationDefinition


class BashRunner(ApplicationDefinition):
    """
    Balsam Bash Runner. P
    """
    environment_variables = {}
    command_template = '/bin/bash -c {{ command }}'
    parameters = {}
    transfers = {}
    
    site = 0

    def postprocess(self):
        from balsam.api import site_config

        workdir = self.job.resolve_workdir(site_config.data_path)
        stdout = workdir.joinpath("job.out").read_text().strip()
        self.job.data = {'result': stdout, 'type': 'bash'}
        self.job.state = "POSTPROCESSED"

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"

class AppRunner(ApplicationDefinition):
    """
    Balsam App Runner for python apps
    """
    environment_variables = {}
    command_template = '{{ python }} app.py'
    parameters = {}
    transfers = {}

    site = 0

    def postprocess(self):
        from balsam.api import site_config
        import json

        workdir = self.job.resolve_workdir(site_config.data_path)
        print('WORKDIR: ', workdir)
        stdout = workdir.joinpath("job.out").read_text().strip()
        metadata = workdir.joinpath("job.metadata").read_text().strip()
        print('STDOUT: ', stdout)

        print("METADATA: ", metadata)
        try:
            metadata = json.loads(metadata)
            self.job.data = metadata
        except:
            import traceback
            print(traceback.format_exc())
            self.job.data = {'result': stdout, 'type': 'bash'}

        self.job.state = "POSTPROCESSED"

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"
        
class ContainerRunner(ApplicationDefinition):
    """
    Container Runner. 
    """
    environment_variables = {}
    command_template = 'singularity exec --bind {{ datadir }}:/data --bind .:/app {{ image }} python /app/app.py'
    parameters = {}
    transfers = {}

    site = 0

    def postprocess(self):
        from balsam.api import site_config
        import json

        workdir = self.job.resolve_workdir(site_config.data_path)
        print('WORKDIR: ', workdir)
        stdout = workdir.joinpath("job.out").read_text().strip()
        metadata = workdir.joinpath("job.metadata").read_text().strip()
        print('STDOUT: ', stdout)

        print("METADATA: ", metadata)
        try:
            metadata = json.loads(metadata)
            self.job.data = metadata
        except:
            import traceback
            print(traceback.format_exc())
            self.job.data = {'result': stdout, 'type': 'bash'}

        self.job.state = "POSTPROCESSED"

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"


class BalsamApp(AppBase):

    def __init__(self, func, data_flow_kernel=None, workdir='work', walltime=60, script='bash', cache=False, executors='BalsamExecutor', ignore_for_cache=None):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache, ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        self.func = func

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        app_fut = dfk.submit(self.func,
                             app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs)

        return app_fut
