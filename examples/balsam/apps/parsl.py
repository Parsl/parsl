from balsam.site import ApplicationDefinition


class BashRunner(ApplicationDefinition):
    """
    Parsl Bash Runner. Place this file in your site directory under apps/ directory.
    """
    environment_variables = {}
    command_template = '/bin/bash -c {{ command }}'
    parameters = {}
    transfers = {}

    def preprocess(self):
        print("COMMAND:",self.command_template)
        self.job.state = "PREPROCESSED"

    def postprocess(self):
        from balsam.api import site_config

        workdir = self.job.resolve_workdir(site_config.data_path)
        stdout = workdir.joinpath("job.out").read_text().strip()
        self.job.data = {'result': stdout, 'type': 'bash'}
        self.job.state = "POSTPROCESSED"

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"


class AppRunner(ApplicationDefinition):
    """
    Parsl App Runner. Place this file in your site directory under apps/ directory.
    """
    environment_variables = {}
    command_template = 'singularity exec --bind {{ workdir }}:/work --bind .:/app {{ image }} {{ command }}'
    parameters = {}
    transfers = {}

    def preprocess(self):
        print("COMMAND:",self.command_template)
        self.job.state = "PREPROCESSED"

    def postprocess(self):
        from balsam.api import site_config
        import json

        workdir = self.job.resolve_workdir(site_config.data_path)
        print('WORKDIR: ', workdir)
        stdout = workdir.joinpath("job.out").read_text().strip()
        metadata = workdir.joinpath("job.metadata").read_text().strip()
        print('STDOUT2: ', stdout)

        print("METADATA: ", metadata)
        try:
            metadata = json.loads(metadata)
            self.job.data = metadata
        except:
            import traceback
            print(traceback.format_exc())
            self.job.data = {'result': stdout, 'type': 'bash'}

        self.job.state = "POSTPROCESSED"

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"