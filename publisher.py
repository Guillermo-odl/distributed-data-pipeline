import urllib.request
import urllib.error
from datetime import datetime

vehicle_id = "1234"  # replace later with real vehicle ID
url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
output_file = "breadcrumbs.json"
data = None

try:
    with urllib.request.urlopen(url) as response:
        data = response.read()

except urllib.error.HTTPError as e:
    print(f"[{datetime.now()}] HTTP Error: {e.code} {e.reason}")
    # print("HTTP Error:", e.code, e.reason) old one 
    data = e.read()

except Exception as e:
    print(f"[{datetime.now()}] Other Error: {e}")
    # print("Other Error:", e) old one
    data = None

if data is not None:
    with open(output_file, "wb") as file:
        file.write(data)
    print(f"[{datetime.now()}] Saved response to {output_file}")
    # print("Saved response to", output_file) old one 
    print(f"[{datetime.now()}] Data Saved: {data.decode('utf-8')}")
    # print("Data Saved:", data.decode("utf-8")[:100]) old one 
else:
    print(f"[{datetime.now()}] No data was saved.")
    # print("No data was saved.") old one

print("-" * 40)
