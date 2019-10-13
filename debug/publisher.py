import os
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/personal-site-staging-a449f/topics/group-chat-messages'

data = '{"name":"Peppy"}'
data = data.encode('utf-8')

future = publisher.publish(topic_path, data=data)

print(future.result())
