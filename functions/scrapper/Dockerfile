FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY utils/ utils/

# Run as non-root user
RUN useradd -m appuser
USER appuser

ENTRYPOINT ["python3", "-u", "main.py"]
CMD []