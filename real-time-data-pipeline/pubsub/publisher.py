from google.cloud import pubsub_v1
import json, time, random

project_id = "your-project-id"
topic_id = "stream-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

while True:
    data = {
        "user_id": f"user_{random.randint(1,100)}",
        "action": random.choice(["click", "purchase", "view"]),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    time.sleep(2)
