import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError

from tqdm import tqdm
import time
import os

# Parameters
INPUT_CSV = "load41_city.csv"
OUTPUT_CSV = "tagged_geolocation_1.csv"
CHECKPOINT_EVERY = 1000
MAX_THREADS = 5  # Keep it below 10 for Nominatim

# Initialize geolocator
geolocator = Nominatim(user_agent="geo_checker", timeout=10)

# Load data
df = pd.read_csv(INPUT_CSV, header=None, names=[
    "id", "city", "city1", "country", "latitude", "longitude", "state"
])
if "geo_accuracy" not in df.columns:
    df["geo_accuracy"] = "unchecked"

# Only check unchecked rows
df_to_process = df[df["geo_accuracy"] == "unchecked"]

def normalize_name(name):
    return str(name).strip().lower().replace(".", "").replace(",", "")

def check_location(index, lat, lon, city, country):
    retries = 3
    for attempt in range(retries):
        try:
            location = geolocator.reverse((lat, lon), language='en')
            if not location or "address" not in location.raw:
                print(f"[{index}] ❌ No address found for coordinates ({lat}, {lon})")
                return index, "unknown"
            
            address = location.raw["address"]

            # Get best guess for city name
            rev_city = address.get("city") or address.get("town") or address.get("village") or address.get("municipality") or address.get("suburb") or ""
            rev_country = address.get("country", "")

            expected_city = normalize_name(city)
            expected_country = normalize_name(country)
            actual_city = normalize_name(rev_city)
            actual_country = normalize_name(rev_country)

            print(f"[{index}] 🔍 Expected: ({expected_city}, {expected_country}) | Actual: ({actual_city}, {actual_country})")

            if expected_city == actual_city and expected_country == actual_country:
                print(f"[{index}] ✅ Match confirmed for city: '{rev_city}', country: '{rev_country}'")
                return index, "accurate"
            else:
                print(f"[{index}] ❌ Mismatch.")
                return index, "inaccurate"
        except GeocoderTimedOut:
            print(f"[{index}] ⏱ Timeout on attempt {attempt + 1}")
            time.sleep(1)
        except Exception as e:
            print(f"[{index}] 🛑 ERROR: {e}")
            break
    return index, "unknown"




# Process in batches
batch = []
for i, row in tqdm(df_to_process.iterrows(), total=len(df_to_process)):
    batch.append((i, row.latitude, row.longitude, str(row.city).lower(), str(row.country).lower()))

    if len(batch) >= CHECKPOINT_EVERY or i == df_to_process.index[-1]:
        print(f"\n🔄 Starting batch of {len(batch)} rows at index {i}...")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {executor.submit(check_location, *entry): entry[0] for entry in batch}
            for future in as_completed(futures, timeout=300):  # 5 minutes max per batch
                index = futures[future]
                try:
                    result_index, result_status = future.result(timeout=30)  # timeout per task
                    df.at[result_index, "geo_accuracy"] = result_status
                except FutureTimeoutError:
                    print(f"[{index}] Task timed out.")
                    df.at[index, "geo_accuracy"] = "timeout"
                except Exception as e:
                    print(f"[{index}] Task failed: {e}")
                    df.at[index, "geo_accuracy"] = "error"

        print(f"✅ Batch completed. Saving checkpoint...")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Checkpoint saved to {OUTPUT_CSV}\n")
        batch = []
