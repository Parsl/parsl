
from globus_sdk import AuthClient, TransferClient, AccessTokenAuthorizer
from globus_sdk.transfer.data import TransferData, TransferData
from globus_sdk.auth import AuthClient


AUTH_ACCESS_TOKEN = "..."
TRANSFER_ACCESS_TOKEN = "..."

# note that we don't provide the client ID in this case
# if you're using an Access Token you can't do the OAuth2 flows
auth_client = AuthClient(authorizer=AccessTokenAuthorizer(AUTH_ACCESS_TOKEN))

tc = TransferClient(
    authorizer=AccessTokenAuthorizer(TRANSFER_ACCESS_TOKEN)
)

# note that we don't provide the client ID in this case
# if you're using an Access Token you can't do the OAuth2 flows
#auth_client = AuthClient(client_id="25ece881-bce3-4cde-afba-138940488466", client_secret="bmLy+cZDsc9CSeP0nL7EtU7Fa4Tb4Gw1mxwQUO/rxNM=")
#rt_authorizer = globus_sdk.RefreshTokenAuthorizer(refresh_token, auth_client)

#transfer_client = globus_sdk.TransferClient(authorizer=rt_authorizer)
#tc = TransferClient(
#    authorizer=AccessTokenAuthorizer(TRANSFER_ACCESS_TOKEN)
#)

tdata = globus_sdk.TransferData(tc, source_endpoint_id, destination_endpoint_id,label="SDK example",sync_level="checksum")
tdata.add_item("/source/path/dir/", "/dest/path/dir/",recursive=True)
tdata.add_item("/source/path/file.txt","/dest/path/file.txt")
transfer_result = tc.submit_transfer(tdata)
print("task_id =", transfer_result["task_id"])


