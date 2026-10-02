FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.2.15 cassandra-driver==3.29.2 requests==2.32.3
WORKDIR /work
COPY configs configs
COPY schema schema
COPY scripts/*.py scripts/
ENTRYPOINT ["python", "scripts/integration.py"]
