#!/usr/bin/env python3
"""
Kafka-Krawler: A tool to crawl Kafka topics and export data to JSON files.
"""

import argparse
import json
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import yaml
except ImportError:
    yaml = None

try:
    from kafka import KafkaConsumer
except ImportError:
    print("Error: kafka-python package not found. Please install it with: pip install kafka-python")
    sys.exit(1)


class KafkaKrawlerConfig:
    """Configuration management for Kafka Krawler."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.bootstrap_servers: List[str] = []
        self.output_dir: str = "output"
        self.timeout_ms: int = 10000
        self.log_level: str = "INFO"
        self.max_workers: int = 4
        
        if config_file and os.path.exists(config_file):
            self.load_config_file(config_file)
    
    def load_config_file(self, config_file: str) -> None:
        """Load configuration from YAML or JSON file."""
        try:
            with open(config_file, 'r') as f:
                if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    if yaml is None:
                        raise ImportError("PyYAML package required for YAML config files. Install with: pip install PyYAML")
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            self.bootstrap_servers = config.get('bootstrap_servers', self.bootstrap_servers)
            self.output_dir = config.get('output_dir', self.output_dir)
            self.timeout_ms = config.get('timeout_ms', self.timeout_ms)
            self.log_level = config.get('log_level', self.log_level)
            self.max_workers = config.get('max_workers', self.max_workers)
            
        except Exception as e:
            logger.error(f"Failed to load config file {config_file}: {e}")
            raise


class KafkaKrawler:
    """Main Kafka crawling class."""
    
    def __init__(self, config: KafkaKrawlerConfig):
        self.config = config
        self.setup_output_directory()
    
    def setup_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        try:
            Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
            logger.info(f"Output directory: {self.config.output_dir}")
        except Exception as e:
            logger.error(f"Failed to create output directory: {e}")
            raise
    
    def process_server_topics(self, server: str) -> None:
        """Process all topics for a given Kafka server."""
        consumer = None
        try:
            logger.info(f"Connecting to Kafka server: {server}")
            consumer = KafkaConsumer(
                bootstrap_servers=[server],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=self.config.timeout_ms
            )
            
            # Get all topics for the current server
            topics = consumer.topics()
            logger.info(f"Found {len(topics)} topics on server {server}")
            
            for topic in topics:
                self.process_topic(consumer, server, topic)
                
        except Exception as e:
            logger.error(f"Error processing server {server}: {e}")
            logger.debug(traceback.format_exc())
        finally:
            if consumer:
                try:
                    consumer.close()
                    logger.debug(f"Closed consumer for server {server}")
                except Exception as e:
                    logger.warning(f"Error closing consumer for {server}: {e}")
    
    def process_topic(self, consumer: KafkaConsumer, server: str, topic: str) -> None:
        """Process a single topic and save messages to file."""
        try:
            logger.info(f"Processing topic: {server} - {topic}")
            consumer.subscribe([topic])
            
            # Clean server name for filename (remove special characters)
            clean_server = server.replace(':', '_').replace('/', '_')
            output_file = Path(self.config.output_dir) / f"{clean_server}_{topic}.json"
            
            messages = consumer.poll(timeout_ms=self.config.timeout_ms)
            message_count = 0
            
            if messages:
                with open(output_file, 'w', encoding='utf-8') as f:
                    for topic_partition, records in messages.items():
                        for record in records:
                            try:
                                # Try to decode as JSON, fallback to string
                                try:
                                    value = json.loads(record.value.decode('utf-8'))
                                except (json.JSONDecodeError, UnicodeDecodeError):
                                    value = record.value.decode('utf-8', errors='replace')
                                
                                if value:
                                    json.dump(value, f, ensure_ascii=False)
                                    f.write('\n')
                                    message_count += 1
                                    
                            except Exception as e:
                                logger.warning(f"Failed to process message in {topic}: {e}")
                
                logger.info(f"Saved {message_count} messages from {server}/{topic} to {output_file}")
            else:
                logger.info(f"No messages found in topic {server}/{topic}")
                
        except Exception as e:
            logger.error(f"Error processing topic {server}/{topic}: {e}")
            logger.debug(traceback.format_exc())
    
    def crawl_all_servers(self) -> None:
        """Crawl all configured Kafka servers concurrently."""
        if not self.config.bootstrap_servers:
            logger.error("No bootstrap servers configured")
            return
        
        logger.info(f"Starting crawl of {len(self.config.bootstrap_servers)} servers with {self.config.max_workers} workers")
        
        try:
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                executor.map(self.process_server_topics, self.config.bootstrap_servers)
            logger.info("Crawling completed successfully")
        except Exception as e:
            logger.error(f"Error during crawling: {e}")
            raise


def load_servers_from_file(filename: str) -> List[str]:
    """Load bootstrap servers from a text file."""
    try:
        with open(filename, 'r') as f:
            servers = [line.strip() for line in f.readlines() if line.strip()]
        logger.info(f"Loaded {len(servers)} servers from {filename}")
        return servers
    except FileNotFoundError:
        logger.error(f"File {filename} not found")
        return []
    except Exception as e:
        logger.error(f"Error reading {filename}: {e}")
        return []


def setup_logging(log_level: str) -> None:
    """Setup logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('kafka-krawler.log'),
            logging.StreamHandler()
        ]
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Kafka Topic Crawler - Extract data from Kafka topics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Crawl a single server
  python kafka-krawler.py --bootstrap-server localhost:9092
  
  # Crawl multiple servers from file
  python kafka-krawler.py --servers-file bootstrap_servers.txt
  
  # Use configuration file
  python kafka-krawler.py --config config.yaml
  
  # Specify output directory and log level
  python kafka-krawler.py --bootstrap-server localhost:9092 --output-dir /tmp/kafka-data --log-level DEBUG
        """
    )
    
    parser.add_argument(
        '--bootstrap-server',
        help='Single Kafka bootstrap server (host:port)'
    )
    
    parser.add_argument(
        '--servers-file',
        default='bootstrap_servers.txt',
        help='File containing list of bootstrap servers (default: bootstrap_servers.txt)'
    )
    
    parser.add_argument(
        '--config',
        help='Configuration file (YAML or JSON)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Output directory for JSON files (default: output)'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=10000,
        help='Consumer timeout in milliseconds (default: 10000)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of concurrent workers (default: 4)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Setup logging first
    setup_logging(args.log_level)
    global logger
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = KafkaKrawlerConfig(args.config)
        
        # Override config with command line arguments
        if args.bootstrap_server:
            config.bootstrap_servers = [args.bootstrap_server]
        elif not config.bootstrap_servers:
            # Try to load from file if no servers configured
            config.bootstrap_servers = load_servers_from_file(args.servers_file)
        
        config.output_dir = args.output_dir
        config.timeout_ms = args.timeout
        config.max_workers = args.max_workers
        config.log_level = args.log_level
        
        if not config.bootstrap_servers:
            logger.error("No bootstrap servers specified. Use --bootstrap-server or provide --servers-file")
            sys.exit(1)
        
        # Start crawling
        krawler = KafkaKrawler(config)
        krawler.crawl_all_servers()
        
    except KeyboardInterrupt:
        logger.info("Crawling interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()