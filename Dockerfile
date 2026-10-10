FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py .

# Private key mounted at runtime from Key Vault secret volume, never baked in
ENV SF_PRIVATE_KEY_PATH=/mnt/secrets/sf-private-key

CMD ["python", "pipeline.py"]
