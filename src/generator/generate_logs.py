# src/generator/generate_logs.py

import uuid
import random
from faker import Faker
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# -----------------------------------
# Initialization
# -----------------------------------
fake = Faker()
spark = SparkSession.builder.appName("LogPulseDataGen").getOrCreate()

NUM_SESSIONS = 20000

# -----------------------------------
# Static Data
# -----------------------------------
devices = ['mobile', 'desktop', 'tablet']
countries = ['IN', 'US', 'UK', 'DE', 'FR']
pages = ['/home', '/product', '/cart', '/checkout', '/search']
referrers = ['google', 'facebook', 'direct', 'instagram', 'email']
event_flow = ['page_view', 'click', 'add_to_cart', 'purchase', 'logout']

event_type_map = {e: i + 1 for i, e in enumerate(event_flow)}

# -----------------------------------
# Session Generator
# -----------------------------------
def generate_session():
    session_id = str(uuid.uuid4())
    user_id = random.randint(1000, 9999)
    device = random.choice(devices)
    device_id = str(uuid.uuid4())
    country = random.choice(countries)
    country_id = countries.index(country) + 1
    ref = random.choice(referrers)

    # Keep product consistent in session
    product_id = random.randint(10000, 10100)

    # Heavy vs casual users
    is_heavy = random.random() < 0.2
    num_events = random.randint(5, 10) if is_heavy else random.randint(2, 5)

    # Peak hour bias (evening heavy traffic)
    base_time = datetime.now() - timedelta(days=random.randint(0, 30))
    hour = random.choices(
        population=list(range(24)),
        weights=[1]*8 + [2]*6 + [5]*6 + [2]*4
    )[0]

    base_time = base_time.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0
    )

    events = []
    current_time = base_time

    for i in range(num_events):
        # Funnel drop-off
        if i > 0 and random.random() < 0.2:
            break

        # Stop after funnel ends
        if i >= len(event_flow):
            break

        event_type = event_flow[i]

        # Time gap between events
        gap = random.randint(5, 120)
        current_time += timedelta(seconds=gap)

        duration = random.randint(5, 300)

        events.append((
            str(uuid.uuid4()),   # event_id
            session_id,
            user_id,
            device,
            device_id,
            event_type,
            event_type_map[event_type],
            random.choice(pages),
            country,
            country_id,
            ref,
            product_id,
            duration,
            current_time
        ))

    return events


# -----------------------------------
# Generate Data
# -----------------------------------
all_data = []

for _ in range(NUM_SESSIONS):
    all_data.extend(generate_session())

# -----------------------------------
# Schema
# -----------------------------------
schema = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("device", StringType()),
    StructField("device_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_type_id", IntegerType()),
    StructField("page_url", StringType()),
    StructField("country", StringType()),
    StructField("country_id", IntegerType()),
    StructField("referrer", StringType()),
    StructField("product_id", IntegerType()),
    StructField("duration_sec", IntegerType()),
    StructField("event_ts", TimestampType())
])

# -----------------------------------
# Create DataFrame & Save
# -----------------------------------
df = spark.createDataFrame(all_data, schema)

df.write.mode("overwrite").parquet("data/raw/logpulse_data.parquet")

print(f"✅ Generated {df.count()} events across {NUM_SESSIONS} sessions")