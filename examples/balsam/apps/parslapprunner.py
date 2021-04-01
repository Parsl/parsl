from balsam.site import ApplicationDefinition


class ParslAppRunner(ApplicationDefinition):
    """
    Parsl App Runner. Place this file in your site directory under apps/ directory.
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
        import json

        workdir = self.job.resolve_workdir(site_config.data_path)
        print('WORKDIR: ', workdir)
        stdout = workdir.joinpath("job.out").read_text().strip()
        print('STDOUT: ', stdout)

        try:
            metadata = json.loads(stdout)
            print("METADATA: ", metadata)
            self.job.data = metadata
        except:
            self.job.data = {'result': stdout, 'type':'bash'}

        self.job.state = "POSTPROCESSED"

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.state = "RESTART_READY"

    def handle_error(self):
        self.job.state = "FAILED"
