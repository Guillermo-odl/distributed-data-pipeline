import json
import time
import gzip
import shutil
import os
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "dataeng-s26-project"
SUBSCRIPTION_ID = "backup_sub"
OUTPUT_DIR = "/home/guiller/pubsub_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

first_breadcrumb_time = None
first_breadcrumb_timestamp = None
num_breadcrumbs = 0
vehicles_with_data = set()
current_date_str = None
current_log_path = None
current_log_file = None


def reset_stats():
    global first_breadcrumb_time
    global first_breadcrumb_timestamp
    global num_breadcrumbs
    global vehicles_with_data
    global current_date_str
    global current_log_path
    global current_log_file

    first_breadcrumb_time = None
    first_breadcrumb_timestamp = None
    num_breadcrumbs = 0
    vehicles_with_data = set()
    current_date_str = None
    current_log_path = None
    current_log_file = None


def open_backup_file_for_today():
    global current_date_str
    global current_log_path
    global current_log_file

    current_date_str = datetime.now().strftime("%Y-%m-%d")
    current_log_path = os.path.join(OUTPUT_DIR, f"breadcrumbs_{current_date_str}.log")
    current_log_file = open(current_log_path, "a", encoding="utf-8")


def ensure_backup_file_open():
    global current_log_file

    if current_log_file is None:
        open_backup_file_for_today()


def process_breadcrumb(record):
    global first_breadcrumb_time
    global first_breadcrumb_timestamp
    global num_breadcrumbs
    global vehicles_with_data
    global current_log_file

    if first_breadcrumb_time is None:
        first_breadcrumb_time = time.time()
        first_breadcrumb_timestamp = datetime.now().isoformat()

    ensure_backup_file_open()

    current_log_file.write(json.dumps(record) + "\n")
    current_log_file.flush()

    num_breadcrumbs += 1

    vehicle_id = record.get("VEHICLE_ID")
    if vehicle_id is not None:
        vehicles_with_data.add(vehicle_id)


def compress_current_file():
    global current_log_file
    global current_log_path

    if current_log_file is not None:
        current_log_file.close()
        current_log_file = None

    if current_log_path is None or not os.path.exists(current_log_path):
        return None, 0

    file_size_bytes = os.path.getsize(current_log_path)
    compressed_path = current_log_path + ".gz"

    with open(current_log_path, "rb") as f_in:
        with gzip.open(compressed_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return compressed_path, file_size_bytes


def print_and_reset_stats():
    compression_timestamp = datetime.now().isoformat()

    compressed_path, file_size_bytes = compress_current_file()

    if first_breadcrumb_time is None:
        walltime = 0
        throughput = 0
    else:
        walltime = time.time() - first_breadcrumb_time
        throughput = num_breadcrumbs / walltime if walltime > 0 else 0

    print(f"BEGIN_TIMESTAMP: {first_breadcrumb_timestamp}")
    print(f"NUM_BREADCRUMBS: {num_breadcrumbs}")
    print(f"NUM_BYTES: {file_size_bytes}")
    print(f"NUM_VEHICLES: {len(vehicles_with_data)}")
    print(f"COMPRESSION_TIMESTAMP: {compression_timestamp}")
    print(f"WALLTIME: {walltime}")
    print(f"THROUGHPUT: {throughput}")

    reset_stats()


def callback(message):
    try:
        if message.attributes.get("message_type") == "sentinel":
            print_and_reset_stats()
            message.ack()
            return

        if message.data == b"SENTINEL":
            print_and_reset_stats()
            message.ack()
            return

        record = json.loads(message.data.decode("utf-8"))
        process_breadcrumb(record)
        message.ack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.ack()


reset_stats()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening on {subscription_path}...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    if current_log_file is not None:
        current_log_file.close()
    streaming_pull_future.cancel()
    print("Backup subscriber stopped.")





