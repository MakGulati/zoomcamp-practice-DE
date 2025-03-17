import csv
import json
from kafka import KafkaProducer
from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Match the bootstrap server in Flink code
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = 'green_tripdata_2019-10.csv'  # change to your CSV file path if needed
    columns_to_keep = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]
    
    count = 0
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        t0 = time()
        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            filtered_row = {col: row[col] for col in columns_to_keep if col in row}
            
            # Convert empty strings to null for numeric fields
            for field in ['passenger_count', 'trip_distance', 'tip_amount']:
                if filtered_row.get(field) == '':
                    filtered_row[field] = None
                elif field in filtered_row:
                    # Try to convert numeric fields to their proper types
                    try:
                        filtered_row[field] = float(filtered_row[field])
                    except (ValueError, TypeError):
                        filtered_row[field] = None
            
            # Ensure integer fields are properly formatted
            for field in ['PULocationID', 'DOLocationID']:
                if field in filtered_row and filtered_row[field]:
                    try:
                        filtered_row[field] = int(filtered_row[field])
                    except (ValueError, TypeError):
                        filtered_row[field] = None
            
            # Send data to Kafka topic "green-trips"
            producer.send('green-trips', value=filtered_row)
            count += 1
            
            # Print progress every 1000 records
            if count % 1000 == 0:
                print(f"Sent {count} records...")

    # Make sure any remaining messages are delivered
    producer.flush()
    t1 = time()
    print(f"Sent {count} messages in {t1 - t0:.2f} seconds")
    producer.close()

if __name__ == "__main__":
    main()