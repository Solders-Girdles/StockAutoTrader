# dataflow/Dockerfile
FROM python:3.9-slim

# Install system dependencies if needed for psycopg2, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (optional but recommended)
RUN useradd --create-home appuser
WORKDIR /home/appuser

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Switch to non-root
USER appuser

# Default command
CMD ["python", "main.py"]