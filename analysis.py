import json
import time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "dataeng-s26-project"
SUBSCRIPTION_ID = "analysis_sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Running stats for the current day's batch
first_breadcrumb_time = None
first_breadcrumb_timestamp = None
min_bc_timestamp = None
max_bc_timestamp = None
num_breadcrumbs = 0
vehicles_with_data = set() # How many unique buses sent data
trip_ids = set() # How many unique trips total


def reset_stats():

    global first_breadcrumb_time
    global first_breadcrumb_timestamp
    global min_bc_timestamp
    global max_bc_timestamp
    global num_breadcrumbs
    global vehicles_with_data
    global trip_ids

    first_breadcrumb_time = None
    first_breadcrumb_timestamp = None
    min_bc_timestamp = None
    max_bc_timestamp = None
    num_breadcrumbs = 0
    vehicles_with_data = set()
    trip_ids = set()


def combine_opd_act_time(opd_date, act_time):

    try:
        date_part = str(opd_date).strip()
        act_seconds = int(act_time)

        hours = act_seconds // 3600
        minutes = (act_seconds % 3600) // 60
        seconds = act_seconds % 60

        return f"{date_part}T{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception:
        return None


def process_breadcrumb(record):

    global first_breadcrumb_time
    global first_breadcrumb_timestamp
    global min_bc_timestamp
    global max_bc_timestamp
    global num_breadcrumbs
    global vehicles_with_data
    global trip_ids

    if first_breadcrumb_time is None:
        first_breadcrumb_time = time.time()
        first_breadcrumb_timestamp = datetime.now().isoformat()

    num_breadcrumbs += 1

    vehicle_id = record.get("VEHICLE_ID")
    if vehicle_id is not None:
        vehicles_with_data.add(vehicle_id)

    trip_id = record.get("EVENT_NO_TRIP")
    if trip_id is not None:
        trip_ids.add(trip_id)

    bc_timestamp = combine_opd_act_time(
        record.get("OPD_DATE"),
        record.get("ACT_TIME")
    )

    if bc_timestamp is not None:
        if min_bc_timestamp is None or bc_timestamp < min_bc_timestamp:
            min_bc_timestamp = bc_timestamp
        if max_bc_timestamp is None or bc_timestamp > max_bc_timestamp:
            max_bc_timestamp = bc_timestamp


def print_and_reset_stats():

    end_timestamp = datetime.now().isoformat()

    if first_breadcrumb_time is None:
        walltime = 0
        throughput = 0
    else:
        walltime = time.time() - first_breadcrumb_time
        throughput = num_breadcrumbs / walltime if walltime > 0 else 0

    print(f"BEGIN_TIMESTAMP: {first_breadcrumb_timestamp}")
    print(f"NUM_VEHICLES: {len(vehicles_with_data)}")
    print(f"MIN_BC_TIMESTAMP: {min_bc_timestamp}")
    print(f"MAX_BC_TIMESTAMP: {max_bc_timestamp}")
    print(f"NUM_TRIPS: {len(trip_ids)}")
    print(f"NUM_BREADCRUMBS: {num_breadcrumbs}")
    print(f"END_TIMESTAMP: {end_timestamp}")
    print(f"WALLTIME: {walltime}")
    print(f"THROUGHPUT: {throughput}")

    reset_stats()


def callback(message):

    try:
        # Detect sentinel from message attribute first
        if message.attributes.get("message_type") == "sentinel":
            print_and_reset_stats()
            message.ack()
            return

        # Fallback: detect raw sentinel payload if needed
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
    streaming_pull_future.cancel()
    print("Analysis subscriber stopped.")
