import logging
import json
import globus_sdk
import os
import parsl
import typeguard

from functools import partial
from typing import Optional
from parsl.app.app import python_app
from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)

# globus staging must be run explicitly in the same process/interpreter
# as the DFK. it relies on persisting globus state between actions in that
# process.

"""
'Parsl Application' OAuth2 client registered with Globus Auth
by lukasz@globusid.org
"""
CLIENT_ID = '8b8060fd-610e-4a74-885e-1051c71ad473'
REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
SCOPES = ('openid '
          'urn:globus:auth:scope:transfer.api.globus.org:all')


get_input = getattr(__builtins__, 'raw_input', input)


def _get_globus_provider(dfk, executor_label):
    if executor_label is None:
        raise ValueError("executor_label is mandatory")
    executor = dfk.executors[executor_label]
    if not hasattr(executor, "storage_access"):
        raise ValueError("specified executor does not have storage_access attribute")
    for provider in executor.storage_access:
        if isinstance(provider, GlobusStaging):
            return provider

    raise Exception('No suitable Globus endpoint defined for executor {}'.format(executor_label))


def get_globus():
    Globus.init()
    return Globus()


class Globus:
    """
    All communication with the Globus Auth and Globus Transfer services is enclosed
    in the Globus class. In particular, the Globus class is responsible for:
     - managing an OAuth2 authorizer - getting access and refresh tokens,
       refreshing an access token, storing to and retrieving tokens from
       .globus.json file,
     - submitting file transfers,
     - monitoring transfers.
    """

    authorizer = None

    @classmethod
    def init(cls):
        token_path = os.path.join(os.path.expanduser('~'), '.parsl')
        if not os.path.isdir(token_path):
            os.mkdir(token_path)
        cls.TOKEN_FILE = os.path.join(token_path, '.globus.json')

        if cls.authorizer:
            return
        cls.authorizer = cls._get_native_app_authorizer(CLIENT_ID)

    @classmethod
    def get_authorizer(cls):
        return cls.authorizer

    @classmethod
    def transfer_file(cls, src_ep, dst_ep, src_path, dst_path):
        tc = globus_sdk.TransferClient(authorizer=cls.authorizer)
        td = globus_sdk.TransferData(tc, src_ep, dst_ep)
        td.add_item(src_path, dst_path)
        try:
            task = tc.submit_transfer(td)
        except Exception as e:
            raise Exception('Globus transfer from {}{} to {}{} failed due to error: {}'.format(
                src_ep, src_path, dst_ep, dst_path, e))

        last_event_time = None
        """
        A Globus transfer job (task) can be in one of the three states: ACTIVE, SUCCEEDED, FAILED.
        Parsl every 15 seconds polls a status of the transfer job (task) from the Globus Transfer service,
        with 60 second timeout limit. If the task is ACTIVE after time runs out 'task_wait' returns False,
        and True otherwise.
        """
        while not tc.task_wait(task['task_id'], timeout=60):
            task = tc.get_task(task['task_id'])
            # Get the last error Globus event
            task_id = task['task_id']
            for event in tc.task_event_list(task_id):
                if event['time'] != last_event_time:
                    last_event_time = event['time']
                    logger.warning(
                        'Non-critical Globus Transfer error event for globus://{}{}: "{}" at {}. Retrying...'.format(
                            src_ep, src_path, event['description'], event['time']))
                    logger.debug('Globus Transfer error details: {}'.format(event['details']))
        """
        The Globus transfer job (task) has been terminated (is not ACTIVE). Check if the transfer
        SUCCEEDED or FAILED.
        """
        task = tc.get_task(task['task_id'])
        if task['status'] == 'SUCCEEDED':
            logger.debug('Globus transfer {}, from {}{} to {}{} succeeded'.format(
                task['task_id'], src_ep, src_path, dst_ep, dst_path))
        else:
            logger.debug('Globus Transfer task: {}'.format(task))
            events = tc.task_event_list(task['task_id'])
            event = events.data[0]
            raise Exception('Globus transfer {}, from {}{} to {}{} failed due to error: "{}"'.format(
                task['task_id'], src_ep, src_path, dst_ep, dst_path, event['details']))

    @classmethod
    def _load_tokens_from_file(cls, filepath):
        with open(filepath, 'r') as f:
            tokens = json.load(f)
        return tokens

    @classmethod
    def _save_tokens_to_file(cls, filepath, tokens):
        with open(filepath, 'w') as f:
            json.dump(tokens, f)

    @classmethod
    def _update_tokens_file_on_refresh(cls, token_response):
        cls._save_tokens_to_file(cls.TOKEN_FILE, token_response.by_resource_server)

    @classmethod
    def _do_native_app_authentication(cls, client_id, redirect_uri,
                                      requested_scopes=None):

        client = globus_sdk.NativeAppAuthClient(client_id=client_id)
        client.oauth2_start_flow(
            requested_scopes=requested_scopes,
            redirect_uri=redirect_uri,
            refresh_tokens=True)

        url = client.oauth2_get_authorize_url()
        print('Please visit the following URL to provide authorization: \n{}'.format(url))
        auth_code = get_input('Enter the auth code: ').strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        return token_response.by_resource_server

    @classmethod
    def _get_native_app_authorizer(cls, client_id):
        tokens = None
        try:
            tokens = cls._load_tokens_from_file(cls.TOKEN_FILE)
        except Exception:
            pass

        if not tokens:
            tokens = cls._do_native_app_authentication(
                client_id=client_id,
                redirect_uri=REDIRECT_URI,
                requested_scopes=SCOPES)
            try:
                cls._save_tokens_to_file(cls.TOKEN_FILE, tokens)
            except Exception:
                pass

        transfer_tokens = tokens['transfer.api.globus.org']

        auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)

        return globus_sdk.RefreshTokenAuthorizer(
            transfer_tokens['refresh_token'],
            auth_client,
            access_token=transfer_tokens['access_token'],
            expires_at=transfer_tokens['expires_at_seconds'],
            on_refresh=cls._update_tokens_file_on_refresh)


class GlobusStaging(Staging, RepresentationMixin):
    """Specification for accessing data on a remote executor via Globus.

    Parameters
    ----------
    endpoint_uuid : str
        Universally unique identifier of the Globus endpoint at which the data can be accessed.
        This can be found in the `Manage Endpoints <https://www.globus.org/app/endpoints>`_ page.
    endpoint_path : str, optional
        FIXME
    local_path : str, optional
        FIXME
    """

    def can_stage_in(self, file):
        logger.debug("Globus checking file {}".format(repr(file)))
        return file.scheme == 'globus'

    def can_stage_out(self, file):
        logger.debug("Globus checking file {}".format(repr(file)))
        return file.scheme == 'globus'

    def stage_in(self, dm, executor, file, parent_fut):
        globus_provider = _get_globus_provider(dm.dfk, executor)
        globus_provider._update_local_path(file, executor, dm.dfk)
        stage_in_app = globus_provider._globus_stage_in_app(executor=executor, dfk=dm.dfk)
        app_fut = stage_in_app(outputs=[file], _parsl_staging_inhibit=True, parent_fut=parent_fut)
        return app_fut._outputs[0]

    def stage_out(self, dm, executor, file, app_fu):
        globus_provider = _get_globus_provider(dm.dfk, executor)
        globus_provider._update_local_path(file, executor, dm.dfk)
        stage_out_app = globus_provider._globus_stage_out_app(executor=executor, dfk=dm.dfk)
        return stage_out_app(app_fu, _parsl_staging_inhibit=True, inputs=[file])

    @typeguard.typechecked
    def __init__(self, endpoint_uuid: str, endpoint_path: Optional[str] = None, local_path: Optional[str] = None):
        self.endpoint_uuid = endpoint_uuid
        self.endpoint_path = endpoint_path
        self.local_path = local_path
        self.globus = None

    def _globus_stage_in_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_globus_stage_in, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    def _globus_stage_out_app(self, executor, dfk):
        executor_obj = dfk.executors[executor]
        f = partial(_globus_stage_out, self, executor_obj)
        return python_app(executors=['_parsl_internal'], data_flow_kernel=dfk)(f)

    # could this happen at __init__ time?
    def initialize_globus(self):
        if self.globus is None:
            self.globus = get_globus()

    def _get_globus_endpoint(self, executor):
        if executor.working_dir:
            working_dir = os.path.normpath(executor.working_dir)
        else:
            raise ValueError("executor working_dir must be specified for GlobusStaging")
        if self.endpoint_path and self.local_path:
            endpoint_path = os.path.normpath(self.endpoint_path)
            local_path = os.path.normpath(self.local_path)
            common_path = os.path.commonpath((local_path, working_dir))
            if local_path != common_path:
                raise Exception('"local_path" must be equal or an absolute subpath of "working_dir"')
            relative_path = os.path.relpath(working_dir, common_path)
            endpoint_path = os.path.join(endpoint_path, relative_path)
        else:
            endpoint_path = working_dir
        return {'endpoint_uuid': self.endpoint_uuid,
                'endpoint_path': endpoint_path,
                'working_dir': working_dir}

    def _update_local_path(self, file, executor, dfk):
        executor_obj = dfk.executors[executor]
        globus_ep = self._get_globus_endpoint(executor_obj)
        file.local_path = os.path.join(globus_ep['working_dir'], file.filename)


# this cannot be a class method, but must be a function, because I want
# to be able to use partial() on it - and partial() does not work on
# class methods
def _globus_stage_in(provider, executor, parent_fut=None, outputs=[], _parsl_staging_inhibit=True):
    globus_ep = provider._get_globus_endpoint(executor)
    file = outputs[0]
    dst_path = os.path.join(
            globus_ep['endpoint_path'], file.filename)

    provider.initialize_globus()

    provider.globus.transfer_file(
            file.netloc, globus_ep['endpoint_uuid'],
            file.path, dst_path)


def _globus_stage_out(provider, executor, app_fu, inputs=[], _parsl_staging_inhibit=True):
    """
    Although app_fu isn't directly used in the stage out code,
    it is needed as an input dependency to ensure this code
    doesn't run until the app_fu is complete. The actual change
    that is represented by the app_fu completing is that the
    executor filesystem will now contain the file to stage out.
    """
    globus_ep = provider._get_globus_endpoint(executor)
    file = inputs[0]
    src_path = os.path.join(globus_ep['endpoint_path'], file.filename)

    provider.initialize_globus()

    provider.globus.transfer_file(
        globus_ep['endpoint_uuid'], file.netloc,
        src_path, file.path
    )


def cli_run():
    parsl.set_stream_logger()
    print("Parsl Globus command-line authorizer")
    print("If authorization to Globus is necessary, the library will prompt you now.")
    print("Otherwise it will do nothing")
    get_globus()
    print("Authorization complete")
