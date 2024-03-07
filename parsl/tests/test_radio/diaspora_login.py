'''
Before using diaspora radio, the user should first login to the diaspora event service.
This can not be aggregated into test file, because it needs an authentication token requiring cli
which pytest does not support.
'''
from diaspora_event_sdk import Client as GlobusClient
c = GlobusClient()
print(c.retrieve_key())
topic = "radio-test"
print(c.register_topic(topic))
print(c.list_topics())
