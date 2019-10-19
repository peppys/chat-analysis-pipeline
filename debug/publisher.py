import os
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/steam-aria-256204/topics/goup-chat-message'

data = '{"name":"Peppy"}'
data = data.encode('utf-8')

future = publisher.publish(topic_path, data=data)

print(future.result())
