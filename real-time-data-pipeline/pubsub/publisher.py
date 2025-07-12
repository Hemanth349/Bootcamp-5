from flask import Flask
import threading
import time
import random
import json
from google.cloud import pubsub_v1
import os

app = Flask(__name__)

project_id = "ancient-cortex-465315-t4"
topic_id = "stream-topic"
# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
# Create the topic path (required by the API)
topic_path = publisher.topic_path(project_id, topic_id)

def publish_messages():
    print("Publishing messages to Pub/Sub...")
    while True:
          # Infinite loop to send messages every 2 seconds
        data = {
            "user_id": f"user_{random.randint(1,100)}",
            "action": random.choice(["click", "purchase", "view"]),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
      # Convert to bytes and publish to Pub/Sub
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
        time.sleep(2)     # Wait for 2 seconds

@app.route("/")
def health_check():
    return "Publisher service is running!", 200

if __name__ == "__main__":
    # Start the publishing loop in a separate thread
    thread = threading.Thread(target=publish_messages, daemon=True)
    thread.start()

    # Run the Flask web server
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


\


  
    
     






