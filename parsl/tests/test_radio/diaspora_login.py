from diaspora_event_sdk import Client as GlobusClient
c = GlobusClient()
print(c.retrieve_key())
topic = "radio-test"
print(c.register_topic(topic))
print(c.list_topics())
