from google.cloud import pubsub_v1
import json, time, random

project_id = "ancient-cortex-465315-t4"
topic_id = "stream-topic"

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Create the topic path (required by the API)
topic_path = publisher.topic_path(project_id, topic_id)

# Infinite loop to send messages every 2 seconds
while True:
      # Create fake JSON data
    data = {
        "user_id": f"user_{random.randint(1,100)}",
        "action": random.choice(["click", "purchase", "view"]),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
       # Convert to bytes and publish to Pub/Sub
    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    time.sleep(2)  # Wait for 2 seconds
