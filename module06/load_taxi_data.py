import csv
import json
from kafka import KafkaProducer
from time import time
def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
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
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        t0 = time()
        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            filtered_row = {col: row[col] for col in columns_to_keep if col in row}
            # Send data to Kafka topic "green-trips"
            producer.send('green-trips', value=filtered_row)
            # print(f"Sent: {filtered_row}")

    # Make sure any remaining messages are delivered
    producer.flush()
    t1 = time()
    print(f"Sent all messages in {t1 - t0:.2f} seconds")
    producer.close()


if __name__ == "__main__":
    main()