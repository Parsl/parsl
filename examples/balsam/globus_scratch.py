import globus_sdk

client = globus_sdk.NativeAppAuthClient("25ece881-bce3-4cde-afba-138940488466")
client.oauth2_start_flow()

authorize_url = client.oauth2_get_authorize_url()
print("Please go to this URL and login: {0}".format(authorize_url))

auth_code = input("Please enter the code you get after login here: ").strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

# the useful values that you want at the end of this
globus_auth_data = token_response.by_resource_server["auth.globus.org"]
globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
globus_auth_token = globus_auth_data["access_token"]
globus_transfer_token = globus_transfer_data["access_token"]
