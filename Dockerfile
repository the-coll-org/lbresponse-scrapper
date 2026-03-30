FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py main.py entrypoint.sh ./
COPY scraper/ scraper/

RUN chmod +x entrypoint.sh && mkdir -p /app/output

ENTRYPOINT ["tini", "--", "./entrypoint.sh"]
CMD ["python", "main.py", "schedule"]
