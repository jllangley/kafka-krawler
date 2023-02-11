# Kafka-Krawler

## This script was designed to iterate through all kafka topics on a server and download the data in each topic. Data will be saved as json files in a {ip_address}.{topic_name}.json format.

### Usage:
- usage for one ip address.
```bash
python3 kafka-krawler.py --bootstrap-server {ip_address}
```
- usage for multiple ip addresses
  - Put your list of servers into a file named "bootstrap_servers.txt"
  - one server per line.
  ```bash
  python3 kafka-krawler.py
  ```

  