FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo 'No packages'

COPY main.py .

CMD ["python", "main.py"]
