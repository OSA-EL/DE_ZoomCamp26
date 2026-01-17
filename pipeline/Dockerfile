FROM python:3.13-slim

WORKDIR /app

RUN pip install pandas pyarrow

COPY ingestData.py .

ENTRYPOINT ["python", "ingestData.py"]


