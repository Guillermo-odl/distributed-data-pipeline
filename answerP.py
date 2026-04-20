import json
import time
from datetime import datetime
import requests
from google.cloud import pubsub_v1

PROJECT_ID = "distributed-data-pipeline"
TOPIC_ID = "bc_topic"

VEHICLE_IDS = [
    2901, 2903, 2905, 2907, 2912, 2914, 2917, 2919, 2925, 2926, 2928, 2937,
    3007, 3008, 3010, 3012, 3013, 3015, 3016, 3018, 3019, 3022, 3026, 3028,
    3029, 3032, 3037, 3039, 3102, 3103, 3104, 3106, 3108, 3115, 3117, 3118,
    3120, 3123, 3124, 3126, 3128, 3130, 3131, 3132, 3133, 3134, 3138, 3139,
    3143, 3146, 3152, 3154, 3155, 3159, 3201, 3204, 3206, 3207, 3212, 3218,
    3221, 3224, 3227, 3228, 3230, 3231, 3234, 3235, 3237, 3240, 3401, 3402,
    3405, 3406, 3407, 3410, 3411, 3412, 3414, 3415, 3417, 3419, 3502, 3504,
    3506, 3507, 3508, 3510, 3512, 3513, 3514, 3515, 3517, 3519, 3520, 3521,
    3524, 3525, 3604, 3606, 3609, 3612, 3615, 3617, 3618, 3621, 3624, 3626,
    3627, 3628, 3704, 3705, 3707, 3712, 3713, 3714, 3715, 3716, 3719, 3720,
    3723, 3724, 3729, 3730, 3803, 3808, 3811, 3812, 3813, 3818, 3819, 3821,
    3822, 3823, 3824, 3825, 3827, 3830, 3905, 3910, 3913, 3915, 3916, 3918,
    3924, 3926, 3927, 3930, 3931, 3932, 3933, 3936, 4004, 4008, 4009, 4011,
    4013, 4019, 4020, 4021, 4023, 4024, 4025, 4026, 4029, 4030, 4105, 4106,
    4107, 4110, 4115, 4117, 4118, 4119, 4121, 4123, 4125, 4128, 4130, 4201,
    4203, 4205, 4208, 4209, 4210, 4212, 4216, 4217, 4218, 4219, 4220, 4222,
    4224, 4225, 4227, 4228, 4229, 4231, 4233, 4234, 4235, 4510, 4511, 4513,
    4520, 4522, 4523, 4529, 4531
]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def main():
    begin_wall = time.time()
    begin_timestamp = datetime.now().isoformat()

    total_breadcrumbs = 0
    vehicles_with_data = set()

    for vehicle_id in VEHICLE_IDS:
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data:
                continue

            vehicles_with_data.add(vehicle_id)

            for breadcrumb in data:
                message = json.dumps(breadcrumb).encode("utf-8")
                future = publisher.publish(topic_path, message)
                future.result()
                total_breadcrumbs += 1

        except Exception as e:
            print(f"ERROR vehicle_id={vehicle_id}: {e}")

    sentinel_future = publisher.publish(topic_path, b"", message_type="sentinel")
    sentinel_future.result()

    end_wall = time.time()
    end_timestamp = datetime.now().isoformat()

    walltime = end_wall - begin_wall
    throughput = total_breadcrumbs / walltime if walltime > 0 else 0.0

    print(f"BEGIN_TIMESTAMP: {begin_timestamp}")
    print(f"NUM_VEHICLES: {len(vehicles_with_data)}")
    print(f"NUM_BREADCRUMBS: {total_breadcrumbs}")
    print(f"WALLTIME: {walltime}")
    print(f"THROUGHPUT: {throughput}")
    print(f"END_TIMESTAMP: {end_timestamp}")


if __name__ == "__main__":
    main()
