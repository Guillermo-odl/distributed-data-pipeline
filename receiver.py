from google.cloud import pubsub_v1
from datetime import datetime
import os

project_id = "dataeng-s26-project"
subscription_id = "breadcrumbs-topic-sub"

output_dir = "/home/guiller/pubsub_data"
os.makedirs(output_dir, exist_ok=True)

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    output_file = os.path.join(output_dir, f"{date_str}.txt")

    data = message.data.decode("utf-8")

    with open(output_file, "a") as f:
        f.write(data + "\n")

    print(f"Saved message to {output_file}")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening on {subscription_path}...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Receiver stopped.")
