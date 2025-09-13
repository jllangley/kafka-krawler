# Multi-stage build for optimal image size
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

# Create non-root user for security
RUN groupadd -r kafka-krawler && useradd -r -g kafka-krawler kafka-krawler

# Set working directory
WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /root/.local /home/kafka-krawler/.local

# Copy application files
COPY kafka-krawler.py .
COPY config.yaml config.json ./

# Create output directory with proper permissions
RUN mkdir -p /app/output /app/logs && \
    chown -R kafka-krawler:kafka-krawler /app

# Switch to non-root user
USER kafka-krawler

# Add local Python packages to PATH
ENV PATH=/home/kafka-krawler/.local/bin:$PATH

# Set Python path to find local packages
ENV PYTHONPATH=/home/kafka-krawler/.local/lib/python3.11/site-packages

# Create volume for output data
VOLUME ["/app/output", "/app/logs"]

# Health check to ensure the application can start
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python kafka-krawler.py --help > /dev/null || exit 1

# Default command (can be overridden)
ENTRYPOINT ["python", "kafka-krawler.py"]
CMD ["--help"]

# Labels for metadata
LABEL maintainer="kafka-krawler"
LABEL description="Kafka topic crawler and data extractor"
LABEL version="2.0"