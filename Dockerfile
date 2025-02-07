FROM python:3.13-slim

WORKDIR /app

COPY backup-sidecar/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backup-sidecar/ .

CMD ["python", "main.py"]
