#!/bin/bash

# Make sure we are in the project root
# cd /path/to/StockAutoTrader   # Uncomment if needed

# Create service directories
mkdir -p dataflow quant riskops execconnect

###############################################################################
# DATAFLOW
###############################################################################
cat << 'EOF' > dataflow/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo 'No packages'

COPY main.py .

CMD ["python", "main.py"]
EOF

cat << 'EOF' > dataflow/main.py
if __name__ == "__main__":
    print("[DataFlow] Starting up...")
EOF

touch dataflow/requirements.txt

###############################################################################
# QUANT
###############################################################################
cat << 'EOF' > quant/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo 'No packages'

COPY main.py .

CMD ["python", "main.py"]
EOF

cat << 'EOF' > quant/main.py
if __name__ == "__main__":
    print("[Quant] Starting up...")
EOF

touch quant/requirements.txt

###############################################################################
# RISKOPS
###############################################################################
cat << 'EOF' > riskops/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo 'No packages'

COPY main.py .

CMD ["python", "main.py"]
EOF

cat << 'EOF' > riskops/main.py
if __name__ == "__main__":
    print("[RiskOps] Starting up...")
EOF

touch riskops/requirements.txt

###############################################################################
# EXECCONNECT
###############################################################################
cat << 'EOF' > execconnect/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo 'No packages'

COPY main.py .

CMD ["python", "main.py"]
EOF

cat << 'EOF' > execconnect/main.py
if __name__ == "__main__":
    print("[ExecConnect] Starting up...")
EOF

touch execconnect/requirements.txt

###############################################################################
# DOCKER-COMPOSE
###############################################################################
cat << 'EOF' > docker-compose.yml
version: "3.8"

services:
  dataflow:
    build: ./dataflow
    container_name: dataflow_service

  quant:
    build: ./quant
    container_name: quant_service
    depends_on:
      - dataflow

  riskops:
    build: ./riskops
    container_name: riskops_service
    depends_on:
      - quant

  execconnect:
    build: ./execconnect
    container_name: execconnect_service
    depends_on:
      - riskops
EOF

###############################################################################
# .GITIGNORE (only if you don't already have one)
###############################################################################
if [ ! -f .gitignore ]; then
cat << 'EOF' > .gitignore
__pycache__/
.env
*.pyc
*.pyo
.vscode/
.idea/
EOF
fi

echo "Setup complete. You can now run 'docker-compose up --build' to test."