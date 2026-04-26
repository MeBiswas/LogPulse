# src/generator/generate_logs.py
import uuid
import random

from faker import Faker
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType, TimeStampType

# Initialization
fake = Faker()
spark = SparkSession.builder.appName("LogPulseDataGen").getOrCreate()

NUM_ROWS = 100,000

# 1. Create a Base DataFrame with IDs
df = spark.range(0, NUM_ROWS)

# 2. Logic for Custom Fields UDFs
def get_uuid():
    return str(uuid.uuid4())

def get_city():
    return fake.city()

def get_timestamp():
    start_time = datetime.now() - timedelta(days=30)
    random_seconds = random.randint(0, 30*24*3600)
    
    return start_time + timedelta(seconds=random.randint(0, 30*24*3600))

# Register UDFs
uuid_udf = udf(get_uuid, StringType())
city_udf = udf(get_city, StringType())
timestamp_udf = udf(get_timestamp, TimeStampType())

# 3. Columns Data
device = ['mobile', 'desktop', 'tablet']
pages = ['/home', '/product', '/cart', '/checkout', '/search']
event_types = ['page_view', 'click', 'add_to_cart', 'purchase', 'logout']

df_final = df.withColumn("session_id", uuid_udf()) \
    .withColumn("timestamp", timestamp_udf()) \
    .withColumn("user_id", (expr("rand() * 9000 + 1000")).cast("int")) \
    .withColumn("event_type", expr(f"element_at(array{tuple(event_types)}, cast(rand() * 5 + 1 as int))")) \
    .withColumn("page_url", expr(f"element_at(array{tuple(pages)}, cast(rand() * 5 + 1 as int))")) \
    .withColumn("location", city_udf())

df_final.write.mode('overwrite').parquet('logpulse_data.parquet')

print("Distributed dataset generated successfully!")

# for _ in range(NUM_ROWS):
#     event = random.choice(event_types)

#     row = {
#         'event_ts': event,
#         'device_id': random.choice(device),
#         'event_type_id': event_types[event],
#         'country': fake.country(),
#         'duration_sec': random.randint(1, 300),
#         'referrer': fake.url(),
#         "product_id": random.randint(100, 500) if event in ["add_to_cart", "purchase"] else None
#     }