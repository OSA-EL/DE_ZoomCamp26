FROM python:3.13-slim

WORKDIR /app

# Install dependencies
RUN pip install pandas sqlalchemy psycopg2-binary tqdm click

# Copy script
COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]
