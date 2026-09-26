# EpiScope runtime image: Python 3.11 + Java 17 (Spark 3.5 supports Java 8/11/17) + pinned requirements.
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless make procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PYTHONPATH=/app PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# pre-fetch the Delta Lake jars at build time so runs work offline
RUN python -c "from pipeline.common.spark import get_spark; get_spark('warmup').stop()" || true
EXPOSE 8765
CMD ["python", "-m", "orchestration.dag", "all"]
