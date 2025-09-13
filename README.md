# Kafka-Krawler

A powerful tool to iterate through all Kafka topics on multiple servers and download the data in each topic. Data is saved as JSON files in `{server}_{topic}.json` format.

## ✨ Features

- **Multi-server support**: Crawl multiple Kafka clusters concurrently
- **Flexible configuration**: Command-line args, config files, or text files
- **Professional logging**: File and console logging with configurable levels
- **Robust error handling**: Graceful error recovery and resource cleanup
- **Type safety**: Full type hints for better code quality
- **Output management**: Configurable output directories

## 🚀 Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## 📖 Usage

### Quick Start

```bash
# Crawl a single server
python3 kafka-krawler.py --bootstrap-server localhost:9092

# Crawl multiple servers from file
echo "localhost:9092" > bootstrap_servers.txt
echo "kafka.example.com:9092" >> bootstrap_servers.txt
python3 kafka-krawler.py --servers-file bootstrap_servers.txt
```

### Advanced Usage

```bash
# Use configuration file
python3 kafka-krawler.py --config config.yaml

# Specify output directory and log level
python3 kafka-krawler.py --bootstrap-server localhost:9092 --output-dir /tmp/kafka-data --log-level DEBUG

# Control concurrency and timeout
python3 kafka-krawler.py --servers-file bootstrap_servers.txt --max-workers 8 --timeout 30000
```

### Configuration File Options

Create a `config.yaml` or `config.json` file:

```yaml
# config.yaml
bootstrap_servers:
  - "localhost:9092"
  - "kafka1.example.com:9092"
output_dir: "output"
timeout_ms: 10000
log_level: "INFO"
max_workers: 4
```

```json
{
  "bootstrap_servers": ["localhost:9092"],
  "output_dir": "output",
  "timeout_ms": 10000,
  "log_level": "INFO",
  "max_workers": 4
}
```

### Command Line Options

```bash
python3 kafka-krawler.py --help
```

- `--bootstrap-server`: Single Kafka server (host:port)
- `--servers-file`: File with list of servers (default: bootstrap_servers.txt)
- `--config`: Configuration file (YAML or JSON)
- `--output-dir`: Output directory (default: output)
- `--timeout`: Consumer timeout in milliseconds (default: 10000)
- `--max-workers`: Concurrent workers (default: 4)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)

## 📁 Output Format

Data is saved as JSON files with the naming convention:
- `{server}_{topic}.json`
- Special characters in server names are replaced with underscores
- Each message is written as a separate JSON line

## 🔧 Requirements

- Python 3.7+
- kafka-python>=2.0.2
- PyYAML>=6.0 (for YAML config files)

## 🐳 Docker Usage

### Quick Start with Docker

```bash
# Build the image
docker build -t kafka-krawler .

# Run with a single server
docker run --rm -v $(pwd)/output:/app/output kafka-krawler --bootstrap-server localhost:9092

# Run with config file
docker run --rm -v $(pwd)/output:/app/output -v $(pwd)/config.yaml:/app/config.yaml kafka-krawler --config /app/config.yaml
```

### Using Docker Compose

The project includes a complete docker-compose setup with Kafka, Zookeeper, and Kafka UI for testing:

```bash
# Start the entire stack (includes local Kafka for testing)
docker-compose up -d

# Run just the crawler against external Kafka
docker-compose run --rm kafka-krawler --bootstrap-server your-kafka-server:9092

# View logs
docker-compose logs kafka-krawler

# Stop everything
docker-compose down
```

### Docker Environment Variables

You can configure the crawler using environment variables in docker-compose.yml:

```yaml
environment:
  - PYTHONUNBUFFERED=1  # For real-time logs
```

### Persistent Data

The Docker setup includes volumes for:
- `/app/output` - Crawled JSON data
- `/app/logs` - Application logs

## 📝 Logging

Logs are written to both console and `kafka-krawler.log` file with timestamps and proper levels for debugging and monitoring.