import logging
import json
import globus_sdk
# from globus_sdk.exc import TransferAPIError


logger = logging.getLogger(__name__)

'''
'Parsl Application' OAuth2 client registered with Globus Auth
by lukasz@globusid.org
'''
CLIENT_ID = '8b8060fd-610e-4a74-885e-1051c71ad473'
REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
SCOPES = ('openid '
          'urn:globus:auth:scope:transfer.api.globus.org:all')

TOKEN_FILE = 'runinfo/.globus.json'


get_input = getattr(__builtins__, 'raw_input', input)


def _load_tokens_from_file(filepath):
    with open(filepath, 'r') as f:
        tokens = json.load(f)
    return tokens


def _save_tokens_to_file(filepath, tokens):
    with open(filepath, 'w') as f:
        json.dump(tokens, f)


def _update_tokens_file_on_refresh(token_response):
    _save_tokens_to_file(TOKEN_FILE, token_response.by_resource_server)


def _do_native_app_authentication(client_id, redirect_uri,
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


def _get_native_app_authorizer(client_id):
    tokens = None
    try:
        tokens = _load_tokens_from_file(TOKEN_FILE)
    except Exception:
        pass

    if not tokens:
        tokens = _do_native_app_authentication(
            client_id=client_id,
            redirect_uri=REDIRECT_URI,
            requested_scopes=SCOPES)
        try:
            _save_tokens_to_file(TOKEN_FILE, tokens)
        except Exception:
            pass

    transfer_tokens = tokens['transfer.api.globus.org']

    auth_client = globus_sdk.NativeAppAuthClient(client_id=client_id)

    return globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=_update_tokens_file_on_refresh)


def get_globus():
    Globus.init()
    return Globus()


class Globus(object):
    """
    All communication with the Globus Auth and Globus Transfer services is enclosed
    in the Globus class. In particular, the Globus class is reponsible for:
     - managing an OAuth2 authorizer - getting access and refresh tokens,
       refreshing an access token, storing to and retrieving tokens from
       .globus.json file,
     - submitting file transfers,
     - monitoring transfers.
    """

    authorizer = None

    @classmethod
    def init(cls):
        if cls.authorizer:
            return
        cls.authorizer = _get_native_app_authorizer(CLIENT_ID)

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
        while not tc.task_wait(task['task_id'], 600, 20):
            task = tc.get_task(task['task_id'])
            events = tc.task_event_list(task['task_id'], num_results=1, filter='is_error:1')
            for evt in events:
                if evt['time'] == last_event_time:
                    break
                last_event_time = evt['time']
                logger.info('Non-critical Globus Transfer error event: {} at {}'.format(evt['description'], evt['time']))
                logger.debug('{}'.format(evt['details']))

        task = tc.get_task(task['task_id'])
        if task['status'] != 'SUCCEEDED':
            logger.error(task)
            raise Exception('Globus transfer {}, from {}{} to {}{} failed due to error: {}'.format(
                task['task_id'], src_ep, src_path, dst_ep, dst_path,
                task['nice_status_short_description']))
        else:
            logger.debug('Globus transfer {}, from {}{} to {}{} succeeded'.format(
                task['task_id'], src_ep, src_path, dst_ep, dst_path))
