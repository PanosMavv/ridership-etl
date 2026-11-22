# Slim runtime image for the ridership + weather ETL pipeline.
#
# Usage (Linux / macOS):
#   docker build -t ridership-etl .
#   docker run --rm -v /path/to/my_data:/data ridership-etl
#   # charts + summary.json appear in /path/to/my_data/output
#
# Usage (Windows PowerShell):
#   docker build -t ridership-etl .
#   docker run --rm -v C:\data:/data ridership-etl
#   # charts appear in C:\data\output
#
# The container expects ridership.csv and weather.db under the mounted
# /data volume (overridable via --input-csv / --weather-db).

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py", "--input-csv", "/data/ridership.csv", "--output-dir", "/data/output"]
