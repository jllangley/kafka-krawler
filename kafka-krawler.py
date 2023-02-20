import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

# Read the list of bootstrap servers from the input file
with open('bootstrap_servers.txt', 'r') as f:
    bootstrap_servers = f.read().splitlines()

def process_server_topics(server):
    # Create a Kafka consumer for the current server
    try:
        consumer = KafkaConsumer(bootstrap_servers=server, auto_offset_reset='earliest')

        # Get a list of all topics for the current server
        topics = consumer.topics()

        # Iterate through each topic
        for topic in topics:
            print(server + " - " + topic)
            consumer.subscribe(topics=topic)
            # Write messages from the current topic to a JSON file named after the server and topic
            messages = consumer.poll(timeout_ms=10000).values()

            if messages:
                with open(f'{server}_{topic}.json', 'w', encoding='utf-8') as f:
                    for message in messages:
                        for record in message:
                            try:
                                value = json.loads(record.value)
                                if value:
                                    f.write(json.dumps(value, indent=4) + '\n')
                            except:
                                print("Decode Error!!")
    except Exception:
        traceback.print_exc()

# Use a ThreadPoolExecutor to run multiple instances of the for loop concurrently
with ThreadPoolExecutor() as executor:
    executor.map(process_server_topics, bootstrap_servers)